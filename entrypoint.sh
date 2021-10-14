#!/bin/bash
set -ex

python3 -c "import funcx; print('funcx Version : ', funcx.__version__)"
python3 -c "import funcx_endpoint; print('funcx_endpoint Version : ', funcx_endpoint.__version__)"
python3 -c "import funcx_forwarder; print('funcx_forwarder Version : ', funcx_forwarder.__version__)"

if [[ -z "${REDIS_HOST}" ]]; then
    REDIS_HOST="$FUNCX_REDIS_MASTER_SERVICE_HOST"
fi

if [[ -z "${RABBITMQ_URI}" ]]; then
    RABBITMQ_URI="amqp://funcx:rabbitmq@${FUNCX_RABBITMQ_SERVICE_HOST}/"
fi

if [[ -z "${ENDPOINT_BASE_PORT}" ]]; then
    ENDPOINT_BASE_PORT=55001
fi

if [[ -z "${RESULT_TTL_SECONDS}" ]]; then
    RESULT_TTL_OPT=""
else
    RESULT_TTL_OPT="--result_ttl $RESULT_TTL_SECONDS"
fi

python3 wait_for_redis.py

if [[ -z "${ADVERTISED_FORWARDER_ADDRESS}" ]]; then
    # If the env var is not set, the assumption is that we are in AWS/EKS and the snippet below
    # fetches the address from the AWS metadata service
    ADVERTISED_FORWARDER_ADDRESS=`wget http://169.254.169.254/latest/meta-data/public-ipv4; cat public-ipv4`
fi


forwarder-service -a $ADVERTISED_FORWARDER_ADDRESS -p 8080 --redishost $REDIS_HOST --redisport $REDIS_PORT --rabbitmquri $RABBITMQ_URI -d --endpoint-base-port ${ENDPOINT_BASE_PORT} $RESULT_TTL_OPT
