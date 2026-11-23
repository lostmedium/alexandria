# pymupdf4llm 2.1 is pinned in requirements.txt; update both when upgrading.
FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processing.py ${LAMBDA_TASK_ROOT}/

CMD ["processing.split_handler"]
