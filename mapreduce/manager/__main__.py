"""MapReduce framework Manager node."""
import sys
import pathlib
import os
import socket
import logging
import json
import time
import click
from mapreduce.utils import tcp_client, tcp_server
from threading import Thread

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

# Configure logging
LOGGER = logging.getLogger(__name__)

# All communication done with strings formatted using JSON
class Manager:
    """Represent a MapReduce framework Manager node."""
    
    def __init__(self, host, port, hb_port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s hb_port=%s pwd=%s",
            host, port, hb_port, os.getcwd(),
        )
        # set up dead variable to end threads
        self.dead = False
        """
        workers: 
        (worker_host, worker_port) => 
        {
            last_checkin : time
            status : {ready, busy, dead}
        }

        threads: [hb_thread, listen_thread, ft_thread]
        """
        self.workers = {}
        self.threads = []
        # set up tmp directory in mapreduce (mapreduce/tmp)
        tmp_path = pathlib.Path(__file__).parents[1] / "tmp"
        tmp_path.mkdir(exist_ok=True)
        # delete any files that are in the dir (if already exists)
        for file in tmp_path.glob('job-*'):
            file.unlink()
        # set up 
        # spwan heart beat thread
        hb_thread = Thread(target=self.check_heartbeats, args=(host, hb_port))
        self.threads.append(hb_thread)
        hb_thread.start()
        # create tcp socket on given port to call listen
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
            while not self.dead:
                message_dict = tcp_server(sock)
                # do something with the message
                if message_dict['message_type'] == 'shutdown':
                    self.shutdown()
                    self.dead = True
                # elif message_dict["message_type"] == "register":
                #     # self.register_worker()
            print("end of listening for manager")

            


    def check_heartbeats(self, host, hb_port):
        """Check for worker heartbeats on UDP."""
        # launch thread to listen for heartbeats and update last_checkin
        args = (host, hb_port)
        hb_listen_thread = Thread(target=self.udp_server, args=args) 
        self.threads.append(hb_listen_thread)
        hb_listen_thread.start()
        # constantly checking for dead workers
        # launch the funeral thread if worker detected dead
        while not self.dead:
            # update worker times and check for death
            curr_time = time.time()
            for worker in self.workers:
                if worker['status'] != 'dead':
                    if curr_time - worker['last_checkin'] > 12:
                        ft_thread = Thread(target=self.fault)
                        ft_thread.start()
                        worker['status'] = 'dead'
        print('end of check_heartbeats')
    
    def register_worker(msg_dict):
        """Add worker to manager's worker dict"""

    def fault():
        """Fault tolerance for managers."""
        print('implement me')

    def udp_server(self, host, port):
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server 
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(1)
            # No sock.listen() since UDP doesn't establish connections like TCP
            # Receive incoming UDP messages
            while not self.dead:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                # update the time that the worker last checked in
                # ignore if not registered 
                key = (message_dict['worker_host'], message_dict['worker_port'])
                # ignore if not registered 
                if key not in self.workers.keys():
                    continue
                self.workers[key]['last_checkin'] = time.time()

    def shutdown(self):
        """Shutdown manager."""
        # send shutdown to all registered workers
        msg = {
            'message_type': 'shutdown'
        }
        for key in self.workers.keys():
            # get workers host and port
            server_host, server_port = key[0], key[1]
            # send the message
            tcp_client(server_host=server_host, server_port=server_port, msg=msg)
            self.workers[key]['status'] = 'dead'
        # kill the manager process
        # TODO: make sure this is the right way to end a process


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
    Manager(host=host, port=port, hb_port=hb_port)

if __name__ == '__main__':
    main()
