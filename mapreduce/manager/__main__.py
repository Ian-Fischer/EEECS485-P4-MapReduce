"""MapReduce framework Manager node."""
import sys
import pathlib
import os
import socket
import logging
import shutil
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
        self.job_counter = 0
        self.queue = []
        self.host = host
        self.port = port
        self.hb_port = hb_port
        self.curr_job = None
        self.workers = {}
        self.threads = []
        """
        workers: 
        (worker_host, worker_port) => 
        {
            last_checkin : time
            status : {ready, busy, dead}
        }

        threads: [hb_thread, listen_thread, ft_thread]
        """
        # set up tmp directory in mapreduce (mapreduce/tmp)
        tmp_path = pathlib.Path("tmp")
        tmp_path.mkdir(exist_ok=True)
        # delete any files that are in the dir (if already exists)
        for thing in tmp_path.glob('job-*'):
            shutil.rmtree(thing)
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
                # registration
                elif message_dict["message_type"] == "register":
                    self.register_worker(message_dict)
                    # TODO: check if there is any work being done and assign it to the worker
                # new job req.
                elif message_dict['message_type'] == 'new_manager_job':
                    # manager recieves this when recieve a new job
                    self.new_job_direc(message_dict, tmp_path)
                    #self.assign_job()
        # now that we are dead, join all the threads
        for thread in self.threads:
            thread.join()


    def assign_job(self, msg_dict):
        # TODO: if not workers or busy, put i nqueue
        # TODO: if there are workers and not busy, start
        if self.available_workers() and (self.curr_job is None):
            #begin job execution
            pass
        else:
            # if there are no workers and not busy, start
            self.queue.append(msg_dict)


            
        print("finish")
    
    def available_workers(self):
        """Check if there are any available workers."""
        for key, worker in self.workers:
            if worker['status'] == 'ready':
                return key
        return None



    def new_job_direc(self, msg_dict, tmp_path):
        """Handle a new job request."""
        """
        {
            "message_type": "new_manager_job",
            "input_directory": string,
            "output_directory": string,
            "mapper_executable": string,
            "reducer_executable": string,
            "num_mappers" : int,
            "num_reducers" : int
        }
        """
        # make intermediate directory
        intrm_dir_path = tmp_path / f"job-{self.job_counter}" / "intermediate"
        intrm_dir_path.mkdir(parents=True, exist_ok=True)
        # get the output directory path
        output_dir_path = pathlib.Path(msg_dict['output_directory'])
        # create the output directory if it does not exist
        output_dir_path.mkdir(parents=True, exist_ok=True)
        # increment job counter
        self.job_counter += 1
        


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
                        self.threads.append(ft_thread)
                        worker['status'] = 'dead'
        print('end of check_heartbeats')
    
    def register_worker(self, msg_dict):
        """Add worker to manager's worker dict"""
        # put the worker in our dictionary (time and info)
        host, port = msg_dict['worker_host'], msg_dict['worker_port']
        self.workers[(host, port)] = {
            'last_checkin': time.time(),
            'status': 'ready'
        }
        # send an ack to the worker
        reg_ack = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port,
        }
        tcp_client(host, port, reg_ack)
        # TODO: check the job queue and assign work if there is any


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
