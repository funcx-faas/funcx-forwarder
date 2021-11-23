#!/usr/bin/env python3

import os
import time

import redis

hostname = os.environ.get("REDIS_HOST")
port = os.environ.get("REDIS_PORT")
connected = False
print(f"Attempting to connect to REDIS at {hostname}:{port}")
for i in range(10):
    try:
        redis_client = redis.StrictRedis(
            host=hostname, port=port, decode_responses=True
        )
        redis_client.ping()
    except Exception:
        print("Trying")
        time.sleep(6)
    else:
        print("Connected!")
        connected = True
        break
if not connected:
    print("Failed to connect to REDIS with 10 attempts over 60s")
    exit(-1)
