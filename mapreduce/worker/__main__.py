"""MapReduce framework Worker node."""
import sys
import os
import logging
import json
import time
import socket
from threading import Thread
import click
from mapreduce.utils import tcp_server, tcp_client


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port,
                manager_hb_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s manager_hb_port=%s",
            manager_host, manager_port, manager_hb_port,
        )
        self.dead = False
        self.ackd = False
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.manager_hb_port = manager_hb_port
        self.threads = []

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
            tcp_client(manager_host, manager_port, reg_msg)
            while not self.dead:
                msg_dict = tcp_server(sock) # get the acknowledgement
                # do something with the message
                if msg_dict['message_type'] == 'shutdown':
                    self.dead = True
                # registration message
                elif msg_dict['message_type'] == 'register_ack':
                    # once we get the ack, set up the heartbeat thread
                    hb_thread = Thread(target=self.udp_client)
                    self.threads.append(hb_thread)
                    hb_thread.start()
                    self.ackd = True
        # now that the worker is dead, join threads
        LOGGER.info('joining all worker threads')
        for thread in self.threads:
            thread.join()

    def udp_client(self):
        """Send worker heartbeats on UDP."""
        heartbeat = {
            "message_type": "heartbeat",
            "worker_host":  self.host,
            "worker_port": self.port
        }
        while not self.dead:
            # Create an INET, DGRAM socket, this is UDP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                # Connect to the UDP socket on server
                sock.connect((self.manager_host, self.manager_hb_port))
                # Send a message
                message = json.dumps(heartbeat)
                sock.sendall(message.encode('utf-8'))
            time.sleep(2)



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
