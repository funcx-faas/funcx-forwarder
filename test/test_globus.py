import argparse
import time
import uuid

from funcx.serialize import FuncXSerializer

from funcx_forwarder.queues import EndpointQueue
from funcx_forwarder.queues.redis.tasks import Task


def slow_double(i, duration=0):
    import time
    time.sleep(duration)
    data = 1
    with open("/home/zzli/.funcx/test1/data/zmq_test.py") as f:
        data = f.readlines()
    return data

def recursive(i, duration=0):
    if duration >= 3:
        return duration
    return recursive(i, duration+1)


def test(endpoint_id=None, tasks=10, duration=1, hostname=None):
    tasks_rq = EndpointQueue(endpoint_id, hostname)
    fxs = FuncXSerializer()

    ser_code = fxs.serialize(slow_double)
    fn_code = fxs.pack_buffers([ser_code])

    tasks_rq.connect()
    start = time.time()
    task_ids = {}
    for i in range(tasks):
        task_id = str(uuid.uuid4())
        ser_args = fxs.serialize([i])
        ser_kwargs = fxs.serialize({'duration': duration})
        input_data = fxs.pack_buffers([ser_args, ser_kwargs])
        payload = fn_code + input_data
        container_id = "RAW"
        task = Task(tasks_rq.redis_client,
                    task_id,
                    container_id,
                    serializer="",
                    data_url="globus://af7bda53-6d04-11e5-ba46-22000b92c6ec/~/globus_test",
                    recursive="True",
                    payload=payload)
        print(task.__dict__)
        tasks_rq.enqueue(task)
        task_ids[i] = task_id

    d1 = time.time() - start
    print("Time to launch {} tasks: {:8.3f} s".format(tasks, d1))

    print(f"Launched {tasks} tasks")
    print(task_ids)
    for i in range(tasks):
        task_id = task_ids[i]
        task = Task.from_id(tasks_rq.redis_client, task_id)
        print(task.__dict__)
        #if task_result or task_exception:
        #    task.delete()
        # TODO: wait for task result...
        # print(task.status, task.header, task.payload, task.result, task.exception)
        while True:
            if task.status == 'success' or task.status == 'failed':
                print(task.status)
                if task.result:
                    print(f"Result: {fxs.deserialize(task.result)}")
                if task.exception:
                    print(fxs.deserialize(task.exception))
                    print(type(fxs.deserialize(task.exception)))
                break
            else:
                print("Current task state: {}".format(task.status))
                time.sleep(2)
            
        # res = results_rq.get('result', timeout=300)
        # print("Result : ", res)

    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(tasks, delta))
    print("Throughput : {:8.3f} Tasks/s".format(tasks / delta))
    return delta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--redis_hostname", default="127.0.0.1",
                        help="Hostname of the Redis server")
    parser.add_argument("-e", "--endpoint_id", required=True,
                        help="Endpoint_id")
    parser.add_argument("-d", "--duration", default=0, type=int,
                        help="Duration of the tasks")
    parser.add_argument("-c", "--count", default=1, type=int,
                        help="Number of tasks")

    args = parser.parse_args()

    test(endpoint_id=args.endpoint_id,
         hostname=args.redis_hostname,
         duration=int(args.duration),
         tasks=int(args.count))
