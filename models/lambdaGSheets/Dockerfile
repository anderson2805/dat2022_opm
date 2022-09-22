FROM public.ecr.aws/lambda/python:3.8
COPY lambda_function.py   ./
COPY metal-imprint-126505-4bb6ab0d6ed2.json   ./
CMD ["lambda_function.lambda_handler"]

COPY requirements.txt ./
RUN pip install -r requirements.txt 