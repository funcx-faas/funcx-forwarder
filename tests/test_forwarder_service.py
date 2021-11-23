import pytest

from funcx_forwarder import service


@pytest.fixture
def client():
    with service.app.test_client() as client:
        yield client


def test_ping(client):
    rv = client.get("/ping")
    print(rv.data)
    assert rv.data == b"pong"
