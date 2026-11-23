import hashlib
import io
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

from botocore.exceptions import ClientError


sys.path.insert(0, str(Path(__file__).parents[1] / "coordination" / "python"))


class FakeCoordination:
    def __init__(self):
        self.states = {}
        self.parts = set()
        self.locks = {}

    @staticmethod
    def source_id(key):
        return hashlib.sha256(key.encode()).hexdigest()

    @staticmethod
    def generation_id(version):
        return hashlib.sha256(version.encode()).hexdigest()

    @contextmanager
    def locked(self, source):
        lock = self.locks.setdefault(source, threading.Lock())
        with lock:
            yield

    def state(self, source):
        return self.states.get(source)

    def put_state(self, source, **values):
        self.states[source] = values

    def mark_part(self, source, generation, part):
        self.parts.add((source, generation, part))

    def clear_part(self, source, generation, part):
        self.parts.discard((source, generation, part))

    def clear_parts(self, source):
        self.parts = {item for item in self.parts if item[0] != source}

    def completed_parts(self, source, generation):
        return sum(item[:2] == (source, generation) for item in self.parts)


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.versions = {}
        self.requests = []
        self.copies = []
        self.delete_error = False
        self.copy_error = False
        self.sequence = 0
        self.lock = threading.RLock()

    def missing(self, key):
        raise ClientError({"Error": {"Code": "NoSuchKey", "Message": key}}, "HeadObject")

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix=""):
        with self.lock:
            keys = sorted(key for bucket, key in self.objects if bucket == Bucket
                          and key.startswith(Prefix))
        return iter({"Contents": [{"Key": key} for key in keys[i:i + 2]]}
                    for i in range(0, len(keys), 2))

    def put_object(self, Bucket, Key, Body, ContentType, Metadata=None):
        with self.lock:
            self.sequence += 1
            version = str(self.sequence)
            item = {
                "body": Body, "metadata": Metadata or {}, "etag": hashlib.md5(Body).hexdigest(),
                "version": version, "content_type": ContentType,
            }
            self.objects[(Bucket, Key)] = item
            self.versions[(Bucket, Key, version)] = item
            return {"VersionId": version}

    def upload_file(self, path, bucket, key, ExtraArgs):
        return self.put_object(bucket, key, Path(path).read_bytes(), **ExtraArgs)

    def head_object(self, Bucket, Key):
        with self.lock:
            item = self.objects.get((Bucket, Key))
            if item is None:
                self.missing(Key)
            return {
                "ETag": item["etag"], "Metadata": item["metadata"],
                "VersionId": item["version"], "ContentType": item["content_type"],
            }

    def get_object(self, **args):
        with self.lock:
            self.requests.append(args)
            bucket, key = args["Bucket"], args["Key"]
            if "VersionId" in args:
                item = self.versions.get((bucket, key, args["VersionId"]))
            else:
                item = self.objects.get((bucket, key))
            if item is None:
                self.missing(key)
            if "IfMatch" in args and args["IfMatch"] != item["etag"]:
                raise ValueError("ETag mismatch")
            return {"Body": io.BytesIO(item["body"]), "Metadata": item["metadata"]}

    def delete_objects(self, Bucket, Delete):
        with self.lock:
            if self.delete_error:
                return {"Errors": [{"Code": "AccessDenied"}]}
            for obj in Delete["Objects"]:
                self.objects.pop((Bucket, obj["Key"]), None)
            return {}

    def copy_object(self, **args):
        with self.lock:
            if self.copy_error:
                raise RuntimeError("copy failed")
            source = args["CopySource"]
            item = self.versions.get((source["Bucket"], source["Key"], source.get("VersionId")))
            if item is None:
                item = self.objects[(source["Bucket"], source["Key"])]
            if item["etag"] != args["CopySourceIfMatch"]:
                raise ValueError("ETag mismatch")
            self.copies.append(args["Key"])
            return self.put_object(
                args["Bucket"], args["Key"], item["body"],
                args.get("ContentType", "application/octet-stream"), args["Metadata"],
            )
