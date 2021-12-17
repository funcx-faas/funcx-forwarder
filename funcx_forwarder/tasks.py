import enum
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


# This internal state is never shown to the user and is meant to track whether
# or not the forwarder has succeeded in fully processing the task
class InternalTaskState(str, enum.Enum):
    INCOMPLETE = "incomplete"
    COMPLETE = "complete"


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

    status = t.cast(TaskState, RedisField(serde=FuncxRedisEnumSerde(TaskState)))
    internal_status = t.cast(
        InternalTaskState, RedisField(serde=FuncxRedisEnumSerde(InternalTaskState))
    )
    user_id = RedisField(serde=INT_SERDE)
    function_id = RedisField()
    endpoint = t.cast(str, RedisField())
    container = RedisField()
    payload = RedisField(serde=JSON_SERDE)
    result = t.cast(t.Optional[str], RedisField())
    result_reference = t.cast(
        t.Optional[t.Dict[str, t.Any]], RedisField(serde=JSON_SERDE)
    )
    data_url = RedisField()
    recursive = RedisField()
    exception = RedisField()
    completion_time = RedisField()
    task_group_id = RedisField()

    # must keep ttl and _set_expire in merge
    # tasks expire in 1 week, we are giving some grace period for
    # long-lived clients, and we'll revise this if there are complaints
    TASK_TTL = int(timedelta(weeks=2).total_seconds())

    def __init__(
        self,
        redis_client: Redis,
        task_id: str,
        user_id: t.Optional[int] = None,
        function_id: t.Optional[str] = None,
        container: t.Optional[str] = None,
        payload: t.Any = None,
        data_url: t.Optional[str] = None,
        recursive:  t.Optional[str] = None,
        task_group_id: t.Optional[str] = None,
    ) -> None:
        """
        If the optional arguments are passed, then they will be written.
        Otherwise, they will fetched from any existing task entry.

        :param redis_client: Redis client so that properties can get get/set
        :param task_id: UUID of task
        :param user_id: ID of user that this task belongs to
        :param function_id: UUID of function for task
        :param container: UUID of container in which to run
        :param payload: serialized function + input data
        :param task_group_id: UUID of task group that this task belongs to
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
        if data_url is not None:
            self.data_url = data_url
        if recursive is not None:
            self.recursive = recursive
        if task_group_id is not None:
            self.task_group_id = task_group_id

        # Used to pass bits of information to EP
        self.header = (
            f"{self.task_id};{self.container};{self.data_url};{self.recursive}"
        )
        self.set_expire(RedisTask.TASK_TTL)

    def set_expire(self, expiration: int):
        """Expires task after expiration(seconds)
        Expiration is set only if 1) there's no expiration set
        or 2) the expiration requested is shorter than current ttl
        This avoids the case where Task objects created from task_id
        could keep extending TTL.
        """
        ttl = self.redis_client.ttl(self.hname)
        if ttl < 0 or expiration < ttl:
            self.redis_client.expire(self.hname, expiration)
        return self.redis_client.ttl(self.hname)

    def delete(self) -> None:
        """Removes this task from Redis, to be used after getting the result"""
        self.redis_client.delete(self.hname)

    @classmethod
    def exists(cls, redis_client: Redis, task_id: str) -> bool:
        """Check if a given task_id exists in Redis"""
        return bool(redis_client.exists(f"task_{task_id}"))
