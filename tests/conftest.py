import os

import pytest
import redis
from funcx_forwarder import service


@pytest.fixture(autouse=True)
def setup_redis_client():
    service.app.config["redis_client"] = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=os.getenv("REDIS_PORT", 6379),
        decode_responses=True,
    )
