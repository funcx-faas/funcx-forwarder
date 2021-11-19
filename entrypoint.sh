#!/bin/bash
if [[ -z "${REDIS_HOST}" ]]; then
    REDIS_HOST="$FUNCX_REDIS_MASTER_SERVICE_HOST"
fi

echo "Starting Forwarder against REDIS server $REDIS_HOST : $REDIS_PORT"
forwarder-service -a $ADVERTISED_FORWARDER_ADDRESS -p 8080 --redishost $REDIS_HOST --redisport $REDIS_PORT --min_ic_port $MIN_IC_PORT --max_ic_port $MAX_IC_PORT

