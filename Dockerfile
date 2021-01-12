# ML http server
#
# VERSION 1.0

FROM python:3.7

LABEL Description="Machine learning http server." \
		Maintainer="Wendy <yuan.liang@smartnews.com>" \
		Version="2.0"

RUN pip install -U awscli

ENV LANG C.UTF-8

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION

WORkDIR /data/app/

COPY . .

RUN pip install -r requirements.txt
RUN [ "python", "-c", "import nltk; nltk.download('punkt')" ] 

RUN mkdir -p /data/app/server/logs

WORKDIR /data/app/server/

ENTRYPOINT ["python", "server.py"]
