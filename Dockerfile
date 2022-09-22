FROM python:3.8.8
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir -r /code/requirements.txt \
    && pip install --no-cache-dir jupyter \
    && pip install --no-cache-dir jupyterlab 
COPY . /code

