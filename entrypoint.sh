#!/bin/bash
set -ex

python3 -c "import funcx; print('funcx Version : ', funcx.__version__)"
python3 -c "import funcx_endpoint; print('funcx_endpoint Version : ', funcx_endpoint.__version__)"
python3 -c "import funcx_forwarder; print('funcx_forwarder Version : ', funcx_forwarder.__version__)"

if [[ -z "${REDIS_HOST}" ]]; then
    REDIS_HOST="$FUNCX_REDIS_MASTER_SERVICE_HOST"
fi

if [[ -z "${RABBITMQ_HOST}" ]]; then
    RABBITMQ_HOST="$FUNCX_RABBITMQ_SERVICE_HOST"
fi

python3 wait_for_redis.py

if [[ -z "${ADVERTISED_FORWARDER_ADDRESS}" ]]; then
    # If the env var is not set, the assumption is that we are in AWS/EKS and the snippet below
    # fetches the address from the AWS metadata service
    ADVERTISED_FORWARDER_ADDRESS=`wget http://169.254.169.254/latest/meta-data/public-ipv4; cat public-ipv4`
fi

forwarder-service -a $ADVERTISED_FORWARDER_ADDRESS -p 8080 --redishost $REDIS_HOST --redisport $REDIS_PORT --rabbitmqhost $RABBITMQ_HOST -d

