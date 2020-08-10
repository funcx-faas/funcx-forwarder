import argparse
import time
import uuid

from funcx.serialize import FuncXSerializer

from funcx_forwarder.queues import EndpointQueue
from funcx_forwarder.queues.redis.tasks import Task


def slow_double(i, duration=0):
    import time
    time.sleep(duration)
    return i * 2


def dont_run_yet(endpoint_id=None, tasks=10, duration=1, hostname=None):
    tasks_rq = EndpointQueue(f'task_{endpoint_id}', hostname)
    fxs = FuncXSerializer()

    ser_code = fxs.serialize(slow_double)
    fn_code = fxs.pack_buffers([ser_code])

    tasks_rq.connect()
    start = time.time()
    for i in range(tasks):
        task_id = str(uuid.uuid4())
        ser_args = fxs.serialize([i])
        ser_kwargs = fxs.serialize({'duration': duration})
        input_data = fxs.pack_buffers([ser_args, ser_kwargs])
        payload = fn_code + input_data
        container_id = "RAW"
        task = Task(tasks_rq.redis_client, task_id, container_id, serializer="", payload=payload)
        tasks_rq.enqueue(task)

    d1 = time.time() - start
    print("Time to launch {} tasks: {:8.3f} s".format(tasks, d1))

    print(f"Launched {tasks} tasks")
    for i in range(tasks):
        res = results_rq.get('result', timeout=300)
        print("Result : ", res)

    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(tasks, delta))
    print("Throughput : {:8.3f} Tasks/s".format(tasks / delta))
    return delta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--redis_hostname", required=True,
                        help="Hostname of the Redis server")
    parser.add_argument("-e", "--endpoint_id", required=True,
                        help="Endpoint_id")
    parser.add_argument("-d", "--duration", required=True,
                        help="Duration of the tasks")
    parser.add_argument("-c", "--count", required=True,
                        help="Number of tasks")

    args = parser.parse_args()

    # test(endpoint_id=args.endpoint_id,
    #      hostname=args.redis_hostname,
    #      duration=int(args.duration),
    #      tasks=int(args.count))
