import typing as t
from datetime import timedelta

from funcx_common.redis import (
    INT_SERDE,
    JSON_SERDE,
    FuncxRedisEnumSerde,
    HasRedisFieldsMeta,
    RedisField,
)
from funcx_common.tasks import TaskProtocol, TaskState
from funcx_endpoint.executors.high_throughput.messages import TaskStatusCode

from redis import Redis


def status_code_convert(code):
    return {
        TaskStatusCode.WAITING_FOR_NODES: TaskState.WAITING_FOR_NODES,
        TaskStatusCode.WAITING_FOR_LAUNCH: TaskState.WAITING_FOR_LAUNCH,
        TaskStatusCode.RUNNING: TaskState.RUNNING,
        TaskStatusCode.SUCCESS: TaskState.SUCCESS,
        TaskStatusCode.FAILED: TaskState.FAILED,
    }[code]


class RedisTask(TaskProtocol, metaclass=HasRedisFieldsMeta):
    """
    ORM-esque class to wrap access to properties of tasks for better style and
    encapsulation
    """

    status = t.cast(
        TaskState, RedisField(serde=FuncxRedisEnumSerde(TaskState))
    )
    user_id = RedisField(serde=INT_SERDE)
    function_id = RedisField()
    endpoint = t.cast(str, RedisField())
    container = RedisField()
    payload = RedisField(serde=JSON_SERDE)
    result = RedisField()
    exception = RedisField()
    completion_time = RedisField()
    task_group_id = RedisField()

    # must keep ttl and _set_expire in merge
    # tasks expire in 1 week, we are giving some grace period for
    # long-lived clients, and we'll revise this if there are complaints
    TASK_TTL = timedelta(weeks=2)

    def __init__(
        self,
        redis_client: Redis,
        task_id: str,
        user_id: t.Optional[int] = None,
        function_id: t.Optional[str] = None,
        container: t.Optional[str] = None,
        payload: t.Any = None,
        task_group_id: t.Optional[str] = None,
    ) -> None:
        """If the kwargs are passed, then they will be overwritten.  Otherwise,
        they will gotten from existing task entry.

        Parameters
        ----------
        rc : StrictRedis
            Redis client so that properties can get get/set
        task_id : str
            UUID of task
        user_id : int
            ID of user that this task belongs to
        function_id : str
            UUID of function for task
        container : str
            UUID of container in which to run
        serializer : str
        payload : str
            serialized function + input data
        task_group_id : str
            UUID of task group that this task belongs to
        """
        self.hname = f"task_{task_id}"
        self.redis_client = redis_client
        self.task_id = task_id

        # If passed, we assume they should be set (i.e. in cases of new tasks)
        # if not passed, do not set
        if user_id is not None:
            self.user_id = user_id
        if function_id is not None:
            self.function_id = function_id
        if container is not None:
            self.container = container
        if payload is not None:
            self.payload = payload
        if task_group_id is not None:
            self.task_group_id = task_group_id

        # Used to pass bits of information to EP
        self.header = f"{self.task_id};{self.container};None"
        self._set_expire()

    def _set_expire(self) -> None:
        """Expires task after TASK_TTL, if not already set."""
        ttl = self.redis_client.ttl(self.hname)
        if ttl < 0:
            # expire was not already set
            self.redis_client.expire(self.hname, RedisTask.TASK_TTL)

    def delete(self) -> None:
        """Removes this task from Redis, to be used after getting the result"""
        self.redis_client.delete(self.hname)

    @classmethod
    def exists(cls, redis_client: Redis, task_id: str) -> bool:
        """Check if a given task_id exists in Redis"""
        return redis_client.exists(f"task_{task_id}")

    @classmethod
    def from_id(cls, redis_client: Redis, task_id: str) -> "RedisTask":
        """For more readable code, use this to find a task by id"""
        return cls(redis_client, task_id)
