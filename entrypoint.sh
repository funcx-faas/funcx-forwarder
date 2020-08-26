#!/bin/bash
echo "Starting Forwarder against REDIS server $REDIS_HOST : $REDIS_PORT"
forwarder-service -a $ADVERTISED_FORWARDER_ADDRESS -p 8080 --redishost $REDIS_HOST --redisport $REDIS_PORT

