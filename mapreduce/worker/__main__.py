"""MapReduce framework Worker node."""
import sys
import os
import logging
import json
import time
import click
import mapreduce.utils
from threading import Thread
import socket


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
        # create a TCP socket and listen()
        """
        
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            # Socket accept() and recv() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            while True:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                # Receive data, one chunk at a time.  If recv() times out before we can
                # read a chunk, then go back to the top of the loop and try again.
                # When the client closes the connection, recv() returns empty data,
                # which breaks out of the loop.  We make a simplifying assumption that
                # the client will always cleanly close the connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                # message_str will be the register_ack
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)
                # upon receiving he register_ack create the heartbeat thread
                hb_thread = Thread(target=Worker.worker_heartbeats)
                hb_thread.start()

        # send the register message to the manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((manager_host, manager_port))
            # send a message
            register_message = {
                "message_type" : "register",
                "worker_host" : host,
                "worker_port" : port
            }
            message = json.dumps(register_message)
            sock.sendall(message.encode('utf-8'))

{
  "message_type": "register_ack",
  "worker_host": string,
  "worker_port": int,
}

        LOGGER.debug("IMPLEMENT ME!")
        time.sleep(120)

    def worker_heartbeats():
        """Send worker heartbeats on UDP."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect((Worker.manager_host, Worker.manager_port))
            # Send a message
            hb_message = {}
            message = json.dumps(hb_message)
            sock.sendall(message.encode('utf-8'))


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
