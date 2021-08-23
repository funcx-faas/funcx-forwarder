import logging
import os
import sys
import zmq
import queue
import requests
import threading
import funcx_forwarder


from multiprocessing import Process, Queue, Event
from funcx_forwarder.taskqueue import TaskQueue
from funcx_forwarder.queues.redis.redis_pubsub import RedisPubSub
from funcx_forwarder.endpoint_db import EndpointDB

from funcx_endpoint.executors.high_throughput.messages import Task, Heartbeat, EPStatusReport, ResultsAck

from funcx_forwarder.queues.redis.tasks import Task as RedisTask
from funcx_forwarder.queues.redis.tasks import TaskState, status_code_convert
import time
import pickle
import pika

logger = logging.getLogger(__name__)


loglevels = {50: 'CRITICAL',
             40: 'ERROR',
             30: 'WARNING',
             20: 'INFO',
             10: 'DEBUG',
             0: 'NOTSET'}


class Forwarder(Process):
    """
                                  +-------------------------+
                                  |         Redis           |
                                  +-----------||------------+
                                     RedisPubSub + hashset
                                              ||
                                  +-----------vv------------+
                                  |                         |
    forwarder-service ---reg_ep---|->      Forwarder        |
                                  |                         |
                                  +-------------------------+
                                  / / /       |     ^     |
                                 / / /        |     |     |
                                / / /         |     |     |
                               / / /       Tasks Results Commands
                              / / /           |     |     |
                             V V V            V     |     V
                       +-------------+     +--------------------+
                       |  Endpoint 1 | ... |     Endpoint N     |
                       +-------------+     +--------------------+


    Endpoint States
    ---------------
    - Registered: Endpoint has been registered with the service but has not yet connected
    - Connected: Endpoint has connected, meaning ZMQ messages can be sent and received
    - Disconnected: ZMQ message sending is failing for this endpoint, making it disconnected

    * All endpoints and endpoint states are forgotten when a forwarder restarts
    * Endpoints always start in the Registered state. Before that, the forwarder does not know they exist
    * Endpoints can send a registration message at any time while in Connected/Disconnected state and it will not impact the current state
    * Results can arrive on the forwarder from an endpoint when it is in any state. See the NOTE below at handle_results

    Endpoint State Transitions
    --------------------------
    Registered -> Connected =>
        Everything is working as expected for first time connection on forwarder
        (Endpoint is registered and ZMQ is working)

    Registered -> nothing =>
        Connectivity issue over the 5500* ports using ZMQ

    Connected -> Disconnected =>
        ZMQ network issues or endpoint is down

    Disconnected -> Connected =>
        Endpoint connection request received. This is either sent when a user manually
        restarts a Disconnected endpoint, or when an endpoint recognizes it has lost
        connection and automatically reconnects
    """

    def __init__(self,
                 command_queue,
                 response_queue,
                 address: str,
                 redis_address: str,
                 rabbitmq_conn_params,
                 endpoint_ports=(55001, 55002, 55003),
                 redis_port: int = 6379,
                 logging_level=logging.INFO,
                 heartbeat_period=30,
                 keys_dir=os.path.abspath('.curve')):
        """
        Parameters
        ----------
        command_queue: Queue
             Queue used by the service to send commands such as 'REGISTER_ENDPOINT'
             Forwarder expects dicts of the form {'command':<TERMINATE/REGISTER_ENDPOINT'> ...}

        response_queue: Queue
             Queue over which responses to commands are returned

        address : str
             Public address at which the forwarder will be accessible from the endpoints

        redis_address : str
             full address to connect to redis. Required

        endpoint_ports : (int, int, int)
             A triplet of ports: (tasks_port, results_port, commands_port)
             Default: (55001, 55002, 55003)

        redis_port : int
             redis port. Default: 6379

        logging_level: int
             Logging level. Default: logging.INFO

        heartbeat_period: int
             heartbeat interval in seconds. Default 2s

        keys_dir: str
             Directory in which curve keys will be stored, Default: '.curve'
        """
        super().__init__()
        self.command_queue = command_queue
        self.response_queue = response_queue
        self.address = address
        self.redis_url = f"{redis_address}:{redis_port}"
        self.rabbitmq_conn_params = rabbitmq_conn_params
        self.tasks_port, self.results_port, self.commands_port = endpoint_ports
        self.connected_endpoints = {}
        self.kill_event = Event()
        self.heartbeat_period = heartbeat_period
        self._last_heartbeat = time.time()
        self.keys_dir = keys_dir
        self.redis_pubsub = RedisPubSub(hostname=redis_address, port=redis_port)
        self.endpoint_db = EndpointDB(hostname=redis_address, port=redis_port)
        self.endpoint_db.connect()

        logger.info(f"Initializing forwarder v{funcx_forwarder.__version__}")
        logger.info(f"Forwarder running on public address: {self.address}")
        logger.info(f"REDIS url: {self.redis_url}")
        logger.info("Log level set to {}".format(loglevels[logging_level]))

        if not os.path.exists(self.keys_dir) or not os.listdir(self.keys_dir):
            logger.info(f"Keys dir empty: {self.keys_dir}, creating keys")
            os.makedirs(self.keys_dir, exist_ok=True)
            forwarder_keyfile, _ = zmq.auth.create_certificates(self.keys_dir, "server")
        else:
            logger.info(f"Keys in {self.keys_dir}: {os.listdir(self.keys_dir)}")
            forwarder_keyfile = os.path.join(self.keys_dir, 'server.key')

        try:
            with open(forwarder_keyfile, 'r') as f:
                self.forwarder_pubkey = f.read()
        except Exception:
            logger.exception(f"[CRITICAL] Failed to read server keyfile from {forwarder_keyfile}")
            raise

    def command_processor(self, kill_event):
        """ command_processor listens on the self.command_queue
        for commands and responds with results on the self.response_queue

        COMMAND messages are dicts of the form:
        {'command' : ['TERMINATE', 'REGISTER_ENDPOINT' ... 'ENDPOINT_LOAD_CONFIG'],
         'id': <ID:int>,
         'options' : ...
        }
        Responses are of the form:
        """
        try:
            while not kill_event.is_set():
                command = self.command_queue.get()
                logger.debug(f"[COMMAND] Received command {command}")
                if command['command'] == 'LIVENESS':
                    response = {'response': True,
                                'id': command.get('id')}
                elif command['command'] == 'TERMINATE':
                    logger.info("[COMMAND] Received TERMINATE command")
                    response = {'response': True,
                                'id': command.get('id')}
                    kill_event.set()
                elif command['command'] == 'REGISTER_ENDPOINT':
                    logger.info("[COMMAND] Received REGISTER_ENDPOINT command")
                    result = self.register_endpoint(command['endpoint_id'],
                                                    command['endpoint_address'],
                                                    command['client_public_key'])

                    response = {'response': result,
                                'id': command.get('id'),
                                'endpoint_id': command['endpoint_id'],
                                'forwarder_pubkey': self.forwarder_pubkey,
                                'public_ip': self.address,
                                'tasks_port': self.tasks_port,
                                'results_port': self.results_port,
                                'commands_port': self.commands_port}

                else:
                    response = {'response': False,
                                'id': command.get('id'),
                                'reason': 'Unknown command'}

                self.response_queue.put(response)
        except Exception:
            logger.exception('Caught exception while processing command')
            sys.exit(-1)

    def register_endpoint(self, endpoint_id, endpoint_address, key):
        """ Add new client keys to the zmq authenticator

        Registering an existing endpoint_id is allowed
        """
        logger.info("endpoint_registered", extra={
            "log_type": "endpoint_registered",
            "endpoint_id": endpoint_id
        })

        self.update_endpoint_metadata(endpoint_id, endpoint_address)

        self.tasks_q.add_client_key(endpoint_id, key)
        self.results_q.add_client_key(endpoint_id, key)
        self.commands_q.add_client_key(endpoint_id, key)
        return True

    def update_endpoint_metadata(self, endpoint_id, endpoint_address):
        """ Geo locate the endpoint and push as metadata into redis
        """
        try:
            resp = requests.get('http://ipinfo.io/{}/json'.format(endpoint_address))
            self.endpoint_db.set_endpoint_metadata(endpoint_id, resp.json())
        except Exception:
            logger.error(f"Failed to geo locate {endpoint_address}")
        else:
            logger.info(f"Endpoint with {endpoint_address} is at {resp}")

    def initialize_endpoint_queues(self):
        ''' Initialize the three queues over which the forwarder communicates with endpoints
        TaskQueue in mode='server' binds to all interfaces by default
        '''
        # SNDTIMEO for the tasks_q should be 0 because we do not want to allow
        # send blocking in the main forwarder loop (an exception will occur instead
        # if the send can't happen immediately)
        self.tasks_q = TaskQueue('127.0.0.1',
                                 port=self.tasks_port,
                                 SNDTIMEO=0,
                                 keys_dir=self.keys_dir,
                                 mode='server')
        self.results_q = TaskQueue('127.0.0.1',
                                   port=self.results_port,
                                   keys_dir=self.keys_dir,
                                   mode='server')
        self.commands_q = TaskQueue('127.0.0.1',
                                    port=self.commands_port,
                                    keys_dir=self.keys_dir,
                                    mode='server')
        return

    def disconnect_endpoint(self, endpoint_id):
        """ Unsubscribes from Redis pubsub and "removes" endpoint from the tasks channel.
        This method does nothing if the endpoint is already disconnected.

        Triggered by zmq messages not getting delivered (heartbeats, tasks, result acks)
        TODO: This needs some extensive testing. It is unclear how well detecting failures
        will work on WAN networks with latencies.
        """
        disconnected_endpoint = self.connected_endpoints.pop(endpoint_id, None)
        # if the endpoint is already disconnected, simply return
        if not disconnected_endpoint:
            return

        logger.info("endpoint_disconnected", extra={
            "log_type": "endpoint_disconnected",
            "endpoint_id": endpoint_id
        })

        self.redis_pubsub.unsubscribe(endpoint_id)

    def add_endpoint_keys(self, ep_id, ep_key):
        """ To remove. this is not used.
        """
        self.tasks_q.add_client_key(ep_key)
        self.results_q.add_client_key(ep_key)
        self.commands_q.add_client_key(ep_key)

    def add_subscriber(self, ep_id):
        self.redis_pubsub.subscribe(ep_id)

    def heartbeat(self):
        """ ZMQ contexts are not thread-safe, heartbeats should happen on the same thread.
        """
        if self._last_heartbeat + self.heartbeat_period > time.time():
            return
        logger.info("Heartbeat")
        dest_endpoint_list = list(self.connected_endpoints.keys())
        for dest_endpoint in dest_endpoint_list:
            logger.debug(f"Sending heartbeat to {dest_endpoint}", extra={
                "log_type": "endpoint_heartbeat_sent",
                "endpoint_id": dest_endpoint
            })
            msg = Heartbeat(endpoint_id=dest_endpoint)
            try:
                self.tasks_q.put(dest_endpoint.encode('utf-8'),
                                 msg.pack())
                self.connected_endpoints[dest_endpoint]['missed_heartbeats'] = 0

            except (zmq.error.ZMQError, zmq.Again):
                logger.exception(f"Endpoint:{dest_endpoint} is unreachable over heartbeats")
                self.disconnect_endpoint(dest_endpoint)
        self._last_heartbeat = time.time()

    def handle_endpoint_connection(self):
        ''' Receive endpoint connection messages. Only connection messages
        are sent from the interchange -> forwarder on the task_q
        '''
        try:
            b_ep_id, reg_message = self.tasks_q.get(block=False, timeout=0)  # timeout in ms # Update to 0ms
            # At this point ep_id is authenticated by means having the client keys.
            ep_id = b_ep_id.decode('utf-8')

            if ep_id in self.connected_endpoints:
                # this is normal, it just means that the endpoint never reached Disconnected
                # state before connecting again
                logger.info(f"[MAIN] Endpoint:{ep_id} attempted connect when it already is in connected list", extra={
                    "log_type": "endpoint_reconnected",
                    "endpoint_id": ep_id
                })
            else:
                logger.info("endpoint_connected", {
                    "log_type": "endpoint_connected",
                    "endpoint_id": ep_id,
                    "registration_message": pickle.loads(reg_message)
                })
                # Now subscribe to messages for ep_id
                # if this endpoint is already in self.connected_endpoints, it is already subscribed
                self.add_subscriber(ep_id)

            self.connected_endpoints[ep_id] = {'registration_message': reg_message,
                                               'missed_heartbeats': 0}
        except zmq.Again:
            pass
        except Exception:
            logger.exception("Caught exception while waiting for registration")

    def log_task_transition(self, task, transition_name):
        extra_logging = {
            "user_id": task.user_id,
            "task_id": task.task_id,
            "task_group_id": task.task_group_id,
            "function_id": task.function_id,
            "endpoint_id": task.endpoint,
            "container_id": task.container,
            "log_type": "task_transition"
        }
        logger.info(transition_name, extra=extra_logging)

    def forward_task_to_endpoint(self):
        ''' Migrates one task from redis to the appropriate endpoint

        Returns:
            int: Count of tasks migrated (0,1)
        '''
        # Now wait for any messages on REDIS that needs forwarding.
        task = None
        try:
            dest_endpoint, task = self.redis_pubsub.get(timeout=0)
            logger.debug(f"Got message from REDIS: {dest_endpoint}:{task}", extra={
                "log_type": "forwarder_redis_task_get",
                "endpoint_id": dest_endpoint
            })
        except queue.Empty:
            return 0
        except Exception:
            logger.exception("Caught exception waiting for message from REDIS")
            return 0

        if dest_endpoint not in self.connected_endpoints:
            # At this point we should be unsubscribed and receiving only messages
            # from the TCP buffers.
            logger.warning(f"Putting back REDIS message for unconnected endpoint: {dest_endpoint}:{task}", extra={
                "log_type": "forwarder_redis_task_put",
                "endpoint_id": dest_endpoint
            })
            self.redis_pubsub.put(dest_endpoint, task)
            self.redis_pubsub.unsubscribe(dest_endpoint)
        else:
            try:
                logger.info(f"Sending task:{task.task_id} to endpoint:{dest_endpoint}")
                zmq_task = Task(task.task_id,
                                task.container,
                                task.payload)
                self.tasks_q.put(dest_endpoint.encode('utf-8'),
                                 zmq_task.pack())
            except (zmq.error.ZMQError, zmq.Again):
                logger.exception(f"Endpoint:{dest_endpoint} is unreachable")
                # put task back in redis since it was not sent to endpoint
                self.redis_pubsub.put(dest_endpoint, task)
                self.disconnect_endpoint(dest_endpoint)
            except Exception:
                logger.exception("Caught error while sending {task.task_id} to {dest_endpoint}")
                # put task back in redis since it was not sent to endpoint
                self.redis_pubsub.put(dest_endpoint, task)
                pass
            else:
                self.log_task_transition(task, 'dispatched_to_endpoint')
        return 1

    def handle_results(self):
        ''' Receive incoming results on results_q and update Redis with results

        NOTE: Results can arrive on this queue when the endpoint is in any of the 3 states
        (Registered, Connected, Disconnected), and we will accept the results. This is because
        we do not tie the connection status of this queue to the state of the endpoint, as
        doing so could mean rejecting perfectly good results on a working ZMQ connection.

        Registered =>
            Getting results in this state means this zmq pipe has opened and results are sent
            over before the connection message has been sent by the endpoint.

        Connected =>
            Getting results in this state is normal, as a connection message has been sent
            and zmq pipes are working.

        Disconnected =>
            Getting results in this state means the endpoint registered and was connected,
            but a zmq send failed over a different pipe, sending the endpoint do Disconnected
            state. The results pipe could still be working, or it could've started working
            again before the connection message is sent again
        '''
        try:
            # timeout in ms, when 0 it's nonblocking
            b_ep_id, b_message = self.results_q.get(block=False, timeout=0)
            endpoint_id = b_ep_id.decode('utf-8')

            if b_message == b'HEARTBEAT':
                logger.debug(f"Received heartbeat from {endpoint_id} over results channel", extra={
                    "log_type": "endpoint_heartbeat_received",
                    "endpoint_id": endpoint_id
                })
                return

            try:
                message = pickle.loads(b_message)
            except Exception:
                logger.exception(f"Failed to unpickle message from results_q, message:{b_message}")

            if isinstance(message, EPStatusReport):
                logger.debug("endpoint_status_message", extra={
                    "log_type": "endpoint_status_message",
                    "endpoint_id": endpoint_id,
                    "endpoint_status_message": message.__dict__
                })
                # Update endpoint status
                try:
                    self.endpoint_db.put(endpoint_id, message.ep_status)
                except Exception:
                    logger.error("Caught error while trying to push endpoint status data into redis")

                # Update task status from endpoint
                task_status_delta = message.task_statuses
                for task_id, status_code in task_status_delta.items():
                    status = status_code_convert(status_code)

                    logger.debug(f"Updating Task({task_id}) to status={status}")
                    task = RedisTask.from_id(self.redis_pubsub.redis_client, task_id)
                    task.status = status
                return

            if 'registration' in message:
                logger.debug(f"Registration message from {message['registration']}")
                return

            task = RedisTask.from_id(self.redis_pubsub.redis_client, message['task_id'])
            logger.debug(f"Task info : {task}")

            # handle if we get duplicate task ids
            prev_task_status = task.status
            if prev_task_status == TaskState.SUCCESS or prev_task_status == TaskState.FAILED:
                logger.debug(f"Duplicate result received for task: {task.task_id}")
                # resend results ack in case the previous ack was not received for this result
                self.handle_results_ack(endpoint_id, task.task_id)
                return

            if 'result' in message:
                task.status = TaskState.SUCCESS
                task.result = message['result']
                task.completion_time = time.time()
            elif 'exception' in message:
                task.status = TaskState.FAILED
                task.exception = message['exception']
                task.completion_time = time.time()

            task_group_id = task.task_group_id
            if ('result' in message or 'exception' in message) and task_group_id:
                connection = pika.BlockingConnection(self.rabbitmq_conn_params)
                channel = connection.channel()
                channel.exchange_declare(exchange='tasks', exchange_type='direct')
                channel.queue_declare(queue=task_group_id)
                channel.queue_bind(task_group_id, 'tasks')

                channel.basic_publish(exchange='tasks', routing_key=task_group_id, body=task.task_id)
                logger.debug(f"Publishing to RabbitMQ routing key {task_group_id} : {task.task_id}")
                connection.close()

                self.log_task_transition(task, 'result_enqueued')

            if 'result' in message or 'exception' in message:
                self.handle_results_ack(endpoint_id, task.task_id)

        except zmq.Again:
            pass
        except Exception:
            logger.exception("Caught exception from results queue")

    def handle_results_ack(self, endpoint_id, task_id):
        if endpoint_id not in self.connected_endpoints:
            logger.warning(f"Attempting to send results Ack to disconnected endpoint: {endpoint_id}", extra={
                "log_type": "disconnected_ack_attempt",
                "endpoint_id": endpoint_id,
                "task_id": task_id
            })

        msg = ResultsAck(task_id=task_id)
        logger.debug(f"Sending Result Ack to endpoint {endpoint_id} for task {task_id}: {msg}", extra={
            "log_type": "results_ack",
            "endpoint_id": endpoint_id,
            "task_id": task_id
        })
        try:
            # send an ack
            self.tasks_q.put(endpoint_id.encode('utf-8'),
                             msg.pack())
        except (zmq.error.ZMQError, zmq.Again):
            logger.exception(f"Endpoint:{endpoint_id} results ack send failed")
            self.disconnect_endpoint(endpoint_id)

    def run(self):
        """ Process entry point
        """
        try:
            logger.info("[MAIN] Loop starting")
            logger.info("[MAIN] Connecting to redis")
            logger.info(f"[MAIN] Forwarder listening for tasks on: {self.tasks_port}")
            logger.info(f"[MAIN] Forwarder listening for results on: {self.results_port}")
            logger.info(f"[MAIN] Forwarder issuing commands on: {self.commands_port}")
            try:
                self.redis_pubsub.connect()
            except Exception:
                logger.exception("[MAIN] Failed to connect to Redis")
                raise

            self.initialize_endpoint_queues()
            self._command_processor_thread = threading.Thread(target=self.command_processor,
                                                              args=(self.kill_event,),
                                                              name="forwarder-command-processor")
            self._command_processor_thread.start()

            while True:
                if self.kill_event.is_set():
                    logger.critical("Kill event set. Starting termination sequence")
                    # 1. [TODO] Unsubscribe from all
                    # 2. [TODO] Flush all tasks received back to their queues for reprocessing.
                    # 3. [TODO] Figure out how we can trigger a scaling event to replace lost forwarder?

                # Send heartbeats to every connected manager
                self.heartbeat()

                self.handle_endpoint_connection()
                # [TODO] This step could be in a timed loop. Ideally after we have a perf study
                self.forward_task_to_endpoint()
                self.handle_results()
        except Exception:
            logger.exception('Caught exception while running forwarder')
            sys.exit(-1)


if __name__ == '__main__':

    command, response = Queue(), Queue()
    fw = Forwarder(command, response, '127.0.0.1')
    fw.run()
