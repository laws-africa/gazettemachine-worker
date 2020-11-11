FROM ubuntu:20.04

COPY requirements.txt /tmp/requirements.txt

RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive \
     apt-get install -y python3-pip \
                        python3-dev \
                        poppler-utils \
                        tesseract-ocr \
                        tesseract-ocr-afr \
                        ghostscript \
  && pip3 install -r /tmp/requirements.txt \
  && rm -rf /tmp/* /var/tmp/*

ADD . /app

WORKDIR /app

ENTRYPOINT ["python3", "worker.py"]
