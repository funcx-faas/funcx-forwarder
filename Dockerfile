FROM python:3.7

# Create a group and user
RUN addgroup funcx && useradd -g funcx funcx

WORKDIR /opt/funcx-forwarder

RUN pip install -U pip
COPY . /opt/funcx-forwarder
RUN pip install .

# @ben, any ideas on why switching user doesn't seem to work here
# USER funcx
WORKDIR /home/funcx

COPY ./wait_for_redis.py .
EXPOSE 55000-55005
EXPOSE 3031
CMD bash /opt/funcx-forwarder/entrypoint.sh

