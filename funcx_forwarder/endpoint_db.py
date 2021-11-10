import json
import logging
import queue
import time
import uuid

import redis
from funcx_common.redis import default_redis_connection_factory

logger = logging.getLogger(__name__)


class EndpointDB:
    """A basic redis DB

    The queue only connects when the `connect` method is called to avoid
    issues with passing an object across processes.

    Parameters
    ----------

    endpoint_id: str
       Endpoint UUID

    hostname : str
       Hostname of the redis server

    port : int
       Port at which the redis server can be reached. Default: 6379

    """

    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client

    def get(self, endpoint_id, timeout=1, last=60 * 4):
        """Get an item from the redis queue

        Parameters
        ----------
        endpoint_id: str
           Endpoint UUID

        timeout : int
           Timeout for the blocking get in seconds
        """
        end = min(self.redis_client.llen(f"ep_status_{endpoint_id}"), last)
        items = self.redis_client.lrange(f"ep_status_{endpoint_id}", 0, end)
        if not items:
            raise queue.Empty
        return items

    def set_endpoint_metadata(self, endpoint_id, json_data):
        """Sets the endpoint metadata in a dict on redis

        Parameters
        ----------

        endpoint_id : str
        Endpoint UUID string

        json_data : {str: str}
        Endpoint metadata as json
        """
        self.redis_client.hmset("endpoint:{}".format(endpoint_id), json_data)

    def put(self, endpoint_id, payload):
        """Put's the key:payload into a dict and pushes the key onto a queue
        Parameters
        ----------
        endpoint_id: str
           Endpoint UUID

        payload : dict
            Dict of task information to be stored
        """
        payload["timestamp"] = time.time()
        # self.redis_client.set(f'{self.prefix}:{key}', json.dumps(payload))
        self.redis_client.lpush(f"ep_status_{endpoint_id}", json.dumps(payload))
        if "new_core_hrs" in payload:
            self.redis_client.incrbyfloat(
                "funcx_worldwide_counter", amount=payload["new_core_hrs"]
            )
        self.redis_client.ltrim(
            f"ep_status_{endpoint_id}", 0, 2880
        )  # Keep 2 x 24hr x 60 min worth of logs

    def __repr__(self):
        return f"EndpointDB({self.redis_client}"


def test():
    rq = EndpointDB(default_redis_connection_factory())
    ep_id = str(uuid.uuid4())
    for i in range(20):
        rq.put(ep_id, {"c": i, "m": i * 100})

    res = rq.get(ep_id, timeout=1)
    print("Result : ", res)


if __name__ == "__main__":
    test()
