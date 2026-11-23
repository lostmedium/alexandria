# pymupdf4llm 2.1 is pinned in requirements.txt; update both when upgrading.
FROM public.ecr.aws/lambda/python:3.13

COPY processing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processing/processing.py ${LAMBDA_TASK_ROOT}/
COPY coordination/python/coordination.py ${LAMBDA_TASK_ROOT}/

CMD ["processing.split_handler"]
