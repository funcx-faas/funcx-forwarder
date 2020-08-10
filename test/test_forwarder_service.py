import pytest
import redis

from funcx_forwarder import service


@pytest.fixture
def client():
    service.app.config['redis_client'] = redis.Redis(
        host='localhost',
        decode_responses=True
    )

    with service.app.test_client() as client:
        yield client


def test_ping(client):
    rv = client.get("/ping")
    print(rv.data)
    assert rv.data == b'pong'
