""" The broker service

This REST service fields incoming registration requests from endpoints,
creates an appropriate forwarder to which the endpoint can connect up.
"""
import argparse
import json
import logging
import pika
import redis
import time
import sys

from flask import Flask, jsonify
from flask import request

from funcx_forwarder.version import VERSION, MIN_EP_VERSION
# from funcx_forwarder.forwarderobject import spawn_forwarder
from funcx_forwarder import set_stream_logger
from funcx_forwarder.forwarder import Forwarder
from multiprocessing import Queue

app = Flask(__name__)
logger = logging.getLogger(__name__)


@app.route('/ping')
def ping():
    """ Minimal liveness response
    """
    return "pong"


@app.route('/version')
def version():
    return jsonify({
        "forwarder": VERSION,
        "min_ep_version": MIN_EP_VERSION
    })


@app.route('/map.json', methods=['GET'])
def get_map_json():
    """ Paint a map of utilization
    """
    results = []
    redis_client = app.config['redis_client']
    for key in redis_client.keys('ep_status_*'):
        try:
            logger.debug("Getting key {}".format(key))
            items = redis_client.lrange(key, 0, 0)
            if items:
                last = json.loads(items[0])
            else:
                continue
            ep_id = key.split('_')[2]
            ep_meta = redis_client.hgetall('endpoint:{}'.format(ep_id))
            lat, lon = ep_meta['loc'].split(',')
            current = {'org': ep_meta['org'].replace(',', '. '),
                       'core_hrs': last['total_core_hrs'],
                       'lat': lat,
                       'long': lon}
            results.append(current)

        except Exception:
            logger.exception(f"Failed to parse for key {key}")

    logger.debug(f"To return : {results}")
    return dict(data=results)


@app.route('/map.csv', methods=['GET'])
def get_map():
    """ Paint a map of utilization
    """
    redis_client = app.config['redis_client']
    csv_string = "org,core_hrs,lat,long\n</br>"
    for key in redis_client.keys('ep_status_*'):
        try:
            logger.debug("Getting key {}".format(key))
            items = redis_client.lrange(key, 0, 0)
            if items:
                last = json.loads(items[0])
            else:
                continue
            ep_id = key.split('_')[2]
            ep_meta = redis_client.hgetall('endpoint:{}'.format(ep_id))
            current = "{},{},{}\n</br>".format(ep_meta['org'].replace(',', '.'), last['total_core_hrs'], ep_meta['loc'])
            csv_string += current

        except Exception:
            logger.exception(f"Failed to parse for key {key}")

    return csv_string


def wait_for_forwarder(fw):
    fw.join()


@app.route('/test/<method>', methods=['GET'])
def test(method):
    logger.debug(f"In test with {method}")
    logger.debug(app.config['forwarder_command'])

    command_id = int(time.time())
    if method == 'TERMINATE':
        command = {'command': 'REGISTER_ENDPOINT',
                   'endpoint_id': 'Foooo',
                   'id': command_id}

    elif method == 'REGISTER_ENDPOINT':
        command = {'command': 'REGISTER_ENDPOINT',
                   'client_keys': 'CLIENT_KEYS',
                   'endpoint_id': 'Foooo',
                   'id': command_id}
    else:
        logger.debug("Unknown method")
        return 'None'

    app.config['forwarder_command'].put(command)
    response = app.config['forwarder_response'].get()

    return response
    # return "Hello"


@app.route('/register', methods=['POST'])
def register():
    """ Register an endpoint request

    1. Register client key with the forwarder instance
    2. Pass connection info back as a json response.
    """
    reg_info = request.get_json()
    endpoint_id = reg_info['endpoint_id']
    logger.debug(f"Registering endpoint : {endpoint_id}", extra={
        "log_type": "forwarder_endpoint_registration_start",
        "endpoint_id": endpoint_id
    })
    app.config['forwarder_command'].put({'command': 'REGISTER_ENDPOINT',
                                         'endpoint_id': reg_info['endpoint_id'],
                                         'endpoint_address': reg_info['endpoint_addr'],
                                         'client_public_key': reg_info.get('client_public_key', None),
                                         'id': 0})
    ret_package = app.config['forwarder_response'].get()
    logger.debug("forwarder_endpoint_registration_response", extra={
        "log_type": "forwarder_endpoint_registration_response",
        "registration_response": ret_package
    })
    return ret_package


@app.route('/list_mappings')
def list_mappings():
    return app.config['ep_mapping']


def cli():
    try:
        cli_run()
    except Exception:
        logger.exception('Caught exception while starting forwarder')
        sys.exit(-1)


def cli_run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", default=8080,
                        help="Port at which the service will listen on")
    parser.add_argument("-a", "--address", required=True,
                        help="Address at which the service is running. This is the address passed to the endpoints")
    parser.add_argument("-c", "--config", default=None,
                        help="Config file")
    parser.add_argument("-r", "--redishost", required=True,
                        help="Redis host address")
    parser.add_argument("--redisport", default=6379,
                        help="Redis port")
    parser.add_argument("--rabbitmqhost", required=True,
                        help="RabbitMQ host address")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")
    parser.add_argument("-v", "--version", action='store_true',
                        help="Print version information")
    parser.add_argument(
        "--endpoint-base-port",
        default=50001,
        type=int,
        help=(
            "The base port for zmq channels.  The forwarder "
            "will reserve three port numbers starting at this "
            "port number and the next two higher."
        ),
    )

    args = parser.parse_args()

    logging_level = logging.DEBUG if args.debug else logging.INFO
    global logger
    logger = set_stream_logger(level=logging_level)

    if args.version is True:
        logger.debug(f"Forwarder version: {VERSION}")
        logger.debug(f"Forwarder minimum endpoint version: {MIN_EP_VERSION}")

    app.config['address'] = args.address
    app.config['ep_mapping'] = {}
    app.config['redis_address'] = args.redishost
    app.config['redis_client'] = redis.StrictRedis(
        host=args.redishost,
        port=int(args.redisport),
        decode_responses=True
    )

    rabbitmq_credentials = pika.PlainCredentials('funcx', 'rabbitmq')
    rabbitmq_conn_params = pika.ConnectionParameters(args.rabbitmqhost, 5672, '/', rabbitmq_credentials)
    app.config['rabbitmq_conn_params'] = rabbitmq_conn_params

    app.config['forwarder_command'] = Queue()
    app.config['forwarder_response'] = Queue()

    fw = Forwarder(app.config['forwarder_command'],
                   app.config['forwarder_response'],
                   args.address,
                   args.redishost,
                   rabbitmq_conn_params,
                   endpoint_ports=range(args.endpoint_base_port, args.endpoint_base_port + 3),
                   logging_level=logging_level,
                   redis_port=args.redisport)
    fw.start()
    app.config['forwarder_process'] = fw

    # Run a test command to make sure the forwarder is online
    app.config['forwarder_command'].put({'command': 'LIVENESS', 'id': 0})
    response = app.config['forwarder_response'].get()
    logger.debug(response)

    # DEBUG ---- <WARNING THIS IS ONLY FOR DEBUG>
    """
    client_key = None
    with open('/tmp/client.key') as f:
        client_key = f.read()
    print("Pushing client key : ", client_key)
    app.config['forwarder_command'].put({'command' : 'REGISTER_ENDPOINT',
                                         'endpoint_id': 'edb1ebd4-f99a-4f99-b8aa-688da5b5ede7',
                                         'client_public_key': client_key,
                                         'id': 0})
    response = app.config['forwarder_response'].get()
    print(f"Registration response : {response}")
    """

    try:
        logger.debug("Starting forwarder service")
        # **WARNING** : DO NOT run this in debug=True mode. It copies the
        # forwarder process into the 2 process mode flask runs when in debug.
        app.run(host="0.0.0.0", port=int(args.port), debug=False)

    except KeyboardInterrupt:
        logger.debug("Exiting from keyboard interrupt")

    except Exception as e:
        # This doesn't do anything
        logger.debug("Caught exception : {}".format(e))
        exit(-1)

    finally:
        logger.debug("Graceful exit")
        app.config['forwarder_command'].put({'command': 'TERMINATE'})
        fw.terminate()


if __name__ == '__main__':
    cli()
