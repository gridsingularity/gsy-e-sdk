FROM python:3.6

ADD . /app

WORKDIR /app

RUN easy_install pip==20.2.4
RUN pip install -e .

ENTRYPOINT ["python"]
