"""MapReduce framework Worker node."""
import sys
import os
import logging
import json
import time
import click
from mapreduce.utils import tcp_server, tcp_client
from threading import Thread
import socket


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port,
                manager_hb_port, threads):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s manager_hb_port=%s",
            manager_host, manager_port, manager_hb_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
        """
        On startup worker should:

        Create a TCP socket on the port and call listen()
        (Only one listen() for lifetime of worker)
        ignore invalid (invalid is fail JSON Decoding)
        try:
            msg = json.loads(msg)
        except JSONDecodeError:
            continue

        Send register message to manager (listening before sending this message)

        when recieve Register_ack message, create a new thread for heartbeat messages (send to manager)

        (Manager should ignore heartbeat from unregistered worker)
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            # Socket accept() and recv() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)

            # send the register message to the manager
            reg_msg = {
                "message_type" : "register",
                "worker_host" : host,
                "worker_port" : port
            }
            reg_thread = Thread(target=tcp_client, args=(manager_host, manager_port, reg_msg,))
            reg_thread.start()
            msg_dict = tcp_server(sock) #get the acknowledgement

            hb_thread = Thread(target=Manager.check_heartbeats)
            threads.append(hb_thread)
            hb_thread.start()


            while msg_dict['message_type'] != "shutdown":
                msg_dict = tcp_server(sock)

            # implement shutdown
            shutdown()

        time.sleep(120)
    def shutdown():
        """Shutdown the Worker after shutdown message received."""
        # once we receive the 




@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--manager-hb-port", "manager_hb_port", default=5999)
def main(host, port, manager_host, manager_port, manager_hb_port):
    """Run Worker."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        f"Worker:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    Worker(host, port, manager_host, manager_port, manager_hb_port)

if __name__ == '__main__':
    main()
