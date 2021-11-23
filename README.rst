FuncX Forwarder Service
=======================

The Forwarder service runs in a container on Flask behind uWSGI.  uWSGI listens on port 3031.

This is a REST micro service that handles requests for initializing Forwarders to which endpoints will connect.
This service will have the following routes:

/register
---------

This route expects a POST with a json payload that identifies the endpoint info and responds with a
json response.

For eg:

POST payload::

  {
     'endpoint_id': '<ENDPOINT_ID>',
     'redis_url': '<REDIS_URL>',
  }


Response payload::

  {
     'endpoint_id': endpoint_id,
     'task_url': 'tcp://55.77.66.22:55001',
     'result_url': 'tcp://55.77.66.22:55002',
     'command_port': 'tcp://55.77.66.22:55003'
  }

/list_endpoints
---------------

This route will list the endpoint mappings.


/ping
-----

This route is for liveness checking. Will return "pong" string when you do a GET on this route.





Architecture and Notes
----------------------

The endpoint registers and receives the information::

  Endpoint / Forwarder Interaction
                                          TaskQ ResultQ
                                             |    |
  REST       /register--> Forwarder----->Executor Client
                ^                            |    ^
                |                            |    |
                |                            v    |
                |          +-------------> Interchange
   User ----> Endpoint ----|
                           +--> Provider



Debugging
=========

You can run the forwareder-service in debug mode on your local system and skip the web-service entirely.
For this, make sure you have the redis package installed and running. You can check this by running:

>>> redis-cli

This should output a prompt that says : 127.0.0.1:6379. This string needs to match.

Now, you can start the forwarder service for testing:

>>> forwarder-service --address 127.0.0.1 --port 50005 --debug

Once you have this running, we can update the endpoint configs to point to this local service.

Docker Container
================

You can run the service inside a docker container. To build the container locally:

.. code-block:: bash

    docker build -t funcx/forwarder:dev .

Then, run the container:

.. code-block:: bash

    # Assumes that Redis and RabbitMQ are running locally in a container
    docker run --rm -it -p 8080:8080 \
      -e "ADVERTISED_FORWARDER_ADDRESS=host.docker.internal" \
      -e "REDIS_HOST=host.docker.internal" \
      -e "REDIS_PORT=6379" \
      -e "FUNCX_RABBITMQ_SERVICE_HOST=host.docker.internal" \
      funcx/forwarder:dev

Once it's running, you can hit the forwarder's endpoints:

.. code-block:: bash

    curl http://localhost:8080/version
