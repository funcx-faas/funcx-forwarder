from funcx_forwarder.queues.redis.redis_pubsub import RedisPubSub
from funcx_endpoint.executors.high_throughput.messages import Task
import argparse
import time


def test_client(ep=None):
    ''' Untested
    '''
    receiver = RedisPubSub('127.0.0.1')
    receiver.connect()
    receiver.subscribe(ep)

    t = None
    delta = None
    count = 0
    while True:
        try:
            msg = receiver.get(timeout=10)

            dest_endpoint, task = msg
            print("Received msg : ", msg)
            if not t:
                t = time.time()
            if task == 'sentinel':
                print("Breaking received sentinel")
                delta = time.time() - t
                break
            else:
                count += 1
        except Exception:
            pass

    print("Performance : {:8.3f} Msgs/s".format(count / delta))


def test_server(ep=None, count=1000, delay=0):
    ''' Untested
    '''
    sender = RedisPubSub('127.0.0.1')
    sender.connect()
    task = Task(sender.redis_client, 'task.0', container='RAW', serializer='', payload='Primer')
    sender.put(ep, task)
    for i in range(count):
        task = Task(sender.redis_client, f'task.{i}', container='RAW', serializer='',
                    payload=f'task body for task.{i}')
        sender.put(ep, task)
        time.sleep(delay)

    task = Task(sender.redis_client, 'sentinel', container='RAW', serializer='',
                payload='sentinel')
    sender.put(ep, task)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--client", action='store_true',
                        help="Run in client mode")
    parser.add_argument("-e", "--endpoint_id", default='testing_ep1',
                        help="Run in client mode")

    args = parser.parse_args()

    if args.client is True:
        test_client(ep=args.endpoint_id)
    else:
        test_server(ep=args.endpoint_id, count=10, delay=10)
