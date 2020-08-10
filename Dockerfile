FROM funcx-web-base

# Create a group and user
RUN addgroup -S uwsgi && adduser -S uwsgi -G uwsgi
RUN pip install --upgrade pip
RUN pip install uwsgi

COPY . /opt/funcx-forwarder
WORKDIR /opt/funcx-forwarder
RUN pip install -q -r ./requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/opt/funcx-forwarder"

USER uwsgi
EXPOSE 55000-56000
CMD sh entrypoint.sh

