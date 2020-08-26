#!/bin/bash
echo "Starting Forwarder against REDIS server $REDIS_HOST : $REDIS_PORT"
forwarder-service -a 0.0.0.0 -p 8080 --redishost $REDIS_HOST --redisport $REDIS_PORT

