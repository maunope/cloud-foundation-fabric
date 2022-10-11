#FROM python:3.8-alpine3.14
FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip && \
    pip3 install --upgrade setuptools

WORKDIR /app

COPY requirements.txt .
COPY main.py .
copy cli.py .

RUN pip3 install -r requirements.txt

ARG project=testvyos
ARG region=europe-west1
ENV PROJECT=${project}
ENV REGION=${region}

ENTRYPOINT python3 cli.py --project $PROJECT --region $REGION