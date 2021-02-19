FROM python:3.7-alpine3.13

RUN apk update && \
    apk add --no-cache gcc musl-dev linux-headers libffi-dev libressl-dev make g++ git python3-dev openssl-dev cargo

# Create a group and user
RUN addgroup -S funcx && adduser -S funcx -G funcx
WORKDIR /opt/funcx-forwarder

RUN pip install -U pip

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . /opt/funcx-forwarder
RUN pip install .

USER funcx
WORKDIR /home/funcx
EXPOSE 55000-56000
EXPOSE 3031
ENTRYPOINT sh /opt/funcx-forwarder/entrypoint.sh

