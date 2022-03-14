"""MapReduce framework Manager node."""
import sys
import pathlib
import os
import socket
import logging
import json
import time
import click
import mapreduce.utils
from threading import Thread

# Configure logging
LOGGER = logging.getLogger(__name__)

# All communication done with strings formatted using JSON
class Manager:
    """Represent a MapReduce framework Manager node."""
    
    def __init__(self, host, port, hb_port, workers):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s hb_port=%s pwd=%s",
            host, port, hb_port, os.getcwd(),
          )
        """
        worker (worker_host, worker_port) = {
            last_checkin (time)
            status (ready, busy, dead)
            worker_host
            worker_port
        }
        """
        # set up tmp directory in mapreduce (mapreduce/tmp)
        tmp_path = pathlib.Path(__file__).parents[1] / "tmp"
        tmp_path.mkdir(exist_ok=True)
        # delete any files that are in the dir (if already exists)
        for file in tmp_path.glob('job-*'):
            file.unlink()
        # set up 
        # make a heartbeat and fault tolerance thread, add to threads
        hb_thread = Thread(target=Manager.manager_heartbeats)
        hb_thread.start()
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be closed when an exception is raised or control flow returns.
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
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)


    def manager_heartbeats():
        """Check for worker heartbeats on UDP."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server 
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((Manager.host, Manager.hb_port))
            sock.settimeout(1)
            # No sock.listen() since UDP doesn't establish connections like TCP
            # Receive incoming UDP messages
            while True:
                # update worker times and check for death
                curr_time = time.time()
                for worker in Manager.workers:
                    if worker['status'] != 'dead':
                        if curr_time - worker['last_checkin'] > 12:
                            ft_thread = Thread(target=Manager.manager_fault)
                            worker['status'] = 'dead'
                            
                # check for any messages
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                # if there is a message, 
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                # update the time that the worker last checked in
                key = (message_dict['worker_host'], message_dict['worker_port'])
                # ignore if not registered 
                if key not in Manager.workers.keys():
                    continue
                Manager.workers[key]['last_checkin'] = time.time()
    
    def manager_fault():
        """Fault tolerance for managers."""
        print('implement me')
                    
                
@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--hb-port", "hb_port", default=5999)
def main(host, port, hb_port):
    """Run Manager."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    Manager(host, port, hb_port, {})
    """
    On startup manager should:
    
    Create new folder temp store all files used by mapreduce server) 
    (if already exists keep it (mkdir) ) (Hints on slash an glob in spec)

    Create new thread to listen for UDP heartbeat from workers

    Create additional processing threads we need (fault tolerance also)

    Create TCP socket on given port and call listen()
    (only one listen() thread for lifetime of manager)

    Wait for incoming messages, ignore invalid (invalid is fail JSON Decoding)
    try:
        msg = json.loads(msg)
    except JSONDecodeError:
        continue

    Return from Manager constructor when all threads have exited
    """

if __name__ == '__main__':
    main()
