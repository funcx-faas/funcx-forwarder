FROM python:3.7-alpine

RUN apk update && \
    apk add --no-cache gcc musl-dev linux-headers libffi-dev libressl-dev make g++

# Create a group and user
RUN addgroup -S uwsgi && adduser -S uwsgi -G uwsgi
RUN pip install --disable-pip-version-check uwsgi
WORKDIR /opt/funcx-forwarder
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . /opt/funcx-forwarder
RUN pip install .

USER uwsgi
WORKDIR /home/uwsgi
EXPOSE 55000-56000
EXPOSE 3031
CMD sh entrypoint.sh

