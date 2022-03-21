"""MapReduce framework Manager node."""
from re import L
import sys
import pathlib
import os
import socket
import logging
import shutil
import json
import time
from threading import Thread
import click
from mapreduce.utils import tcp_client, tcp_server

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
        self.curr_job = {}
        self.curr_job_m = {}
        self.curr_job_r = {}
        self.stage = None
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
                LOGGER.info(f'Current Message: {message_dict}')
                # do something with the message
                if message_dict['message_type'] == 'shutdown':
                    self.shutdown()
                    self.dead = True
                # registration
                elif message_dict["message_type"] == 'register':
                    self.register_worker(message_dict)
                # new job req.
                elif message_dict['message_type'] == 'new_manager_job':
                    # manager recieves this when recieve a new job
                    self.new_job_direc(message_dict, tmp_path)
                elif message_dict['message_type'] == 'finished':
                    # deal with a finished message
                    self.finished(message_dict)
        # now that Manager is dead, join all the threads
        LOGGER.info('joining manager all threads')
        for thread in self.threads:
            LOGGER.info(f'closing thread {thread.name}')
            thread.join()

    def finished(self, msg_dict):
        """Deal with received finished message."""
        # set task to finished, add output paths to the dictionary
        worker = (msg_dict['worker_host'], msg_dict['worker_port'])
        LOGGER.info(f'Finished task #{msg_dict["task_id"]}')
        if self.stage == 'map':
            task_id, output_paths = msg_dict['task_id'], msg_dict['output_paths']
            self.curr_job_m[task_id]['status'] = 'done'
            self.curr_job_m[task_id]['output_paths'] = output_paths
            self.workers[worker]['status'] = 'ready'
        else:
            task_id, output_paths = msg_dict['task_id'], msg_dict['output_paths']
            self.curr_job_r[task_id]['status'] = 'done'
            self.curr_job_r[task_id]['output_paths'] = output_paths
            self.workers[worker]['status'] = 'ready'

    def stage_finished(self):
        """Check if there are any more tasks left in curr stage."""
        # map stage
        if self.stage == 'map':
            for _, value in self.curr_job_m.items():
                if value['status'] in ['no', 'busy']:
                    return False
            return True
        # reduce stage
        for _, value in self.curr_job_r.items():
            if value['status'] in ['no', 'busy']:
                return False
        return True

    def execute_job(self):
        """Function that will execute given job."""
        LOGGER.info("Manager:%s, begin map stage", self.port)
        # 1. partition the input
        self.partition_mapper()
        # 2. map
        while not self.stage_finished():
            for key, worker in self.workers.items():
                if worker['status'] == 'ready':
                    #send the worker work if there is any
                    taskid = self.work()
                    if taskid is not None:
                        LOGGER.info(f'Assigning worker {key}, task is {taskid}')
                        self.assign_task(taskid=taskid, worker=key)
        LOGGER.info("Manager:%s, end map stage", self.port)
        # 3. reduce
        self.stage = 'reduce'
        # 4. exit

    
    def available_workers(self):
        """Check if there are any available workers."""
        for _, worker in self.workers.items():
            if worker['status'] == 'ready':
                return True
        return False

    
    def partition_mapper(self):
        """Partition files and create curr_job_m."""
        input_files = os.listdir(self.curr_job['input_directory']) # should it be an input_path instread?
        input_files.sort()
        num_mappers = self.curr_job['num_mappers'] 
        # assign the input_file to it's task
        part_files = []
        for _ in range(num_mappers):
            part_files.append([])
        # round-robin
        for i, file_path in enumerate(input_files):
            LOGGER.info(f'PARTITIONING i={i}, i mod mappers={i%num_mappers}')
            task_id = i % num_mappers
            part_files[task_id].append(self.curr_job['input_directory']+'/'+file_path)
            LOGGER.info(f'PARTITIONING adding file={file_path} to task={task_id}, part={part_files}')
        for taskid in range(num_mappers):
            self.curr_job_m[taskid] = {
                "status": "no",
                "input_files": part_files[taskid],
                "executable": self.curr_job['mapper_executable'],
                "output_directory": str(self.curr_job['intermediate']),
                "num_partitions": self.curr_job['num_reducers'],
                "worker_host": None,
                "worker_port": None
            }
        LOGGER.info(f'Done partitioning: {self.curr_job_m}')
            

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
        msg_dict['intermediate'] = intrm_dir_path
        # get the output directory path
        output_dir_path = pathlib.Path(msg_dict['output_directory'])
        # create the output directory if it does not exist
        output_dir_path.mkdir(parents=True, exist_ok=True)
        # increment job counter
        self.job_counter += 1
        # only execute job if no curr job and there are available workers
        LOGGER.info(f'Received new job, current job status is {not self.curr_job}, available workers is {self.available_workers()}')
        if not self.curr_job and self.available_workers():
            # update member variables
            LOGGER.info('Assigned new job!')
            self.stage = 'map'
            self.curr_job = msg_dict
            job_thread = Thread(target=self.execute_job)
            job_thread.start()
            self.threads.append(job_thread)
        else:
            self.queue.append(msg_dict)


    def work(self):
        """Check if there are any unassigned partitions in curr_job."""
        """
        (taskid)
        {
            "status": {no, busy, done}
            "input_files": []
            "executable": <executable>
            "output_dir": <directory>
            "worker_host": assigned worker host
            "worker_port": assigned worker port
        }
        """
        if self.stage == 'map':
            partitions = self.curr_job_m
        else:
            partitions = self.curr_job_r
        for taskid, partition in partitions.items():
            if partition['status'] == 'no':
                return taskid
        return None
        

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
            for worker in self.workers.values():
                if worker['status'] != 'dead':
                    if curr_time - worker['last_checkin'] > 12:
                        ft_thread = Thread(target=self.fault)
                        ft_thread.start()
                        self.threads.append(ft_thread)
                        worker['status'] = 'dead'
    
    
    def assign_task(self, taskid, worker):
        """Assign a task to a worker."""
        """
        (taskid)
        {
            "status": {no, busy, done}
            "input_files": []
            "executable": <executable>
            "output_dir": <directory>
            "worker_host": assigned worker host
            "worker_port": assigned worker port
        }
        """
        if self.stage == 'map':
            # create the message
            task_message = {
                "message_type": "new_map_task",
                "task_id": taskid,
                "input_paths": self.curr_job_m[taskid]['input_files'],
                "executable": self.curr_job_m[taskid]['executable'],
                "output_directory": self.curr_job_m[taskid]['output_directory'],
                "num_partitions": self.curr_job['num_reducers'],
                "worker_host": worker[0],
                "worker_port": worker[1]
            }
            # send the message to the worker
            LOGGER.info(f'Sending task {taskid} to worker {worker}')
            tcp_client(server_host=worker[0], server_port=worker[1], msg=task_message)
            LOGGER.info(f'sent!')
            # update the partition for the worker info
            self.curr_job_m[taskid]['worker_host'] = worker[0]
            self.curr_job_m[taskid]['worker_port'] = worker[1]
            self.curr_job_m[taskid]['status'] = 'busy'
            self.workers[worker]['status'] = 'busy'
            LOGGER.info(f'new task: {self.curr_job_m[taskid]}')
        # TODO: elif self.stage == 'reduce':

    def register_worker(self, msg_dict):
        """Add worker to manager's worker dict"""
        # put the worker in our dictionary (time and info)
        host, port = msg_dict['worker_host'], msg_dict['worker_port']
        self.workers[(host, port)] = {
            'last_checkin': time.time(),
            'status': 'ready'
        }
        LOGGER.info(f'registered new worker, host:{host} port: {port}')
        # send an ack to the worker
        reg_ack = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port,
        }
        tcp_client(host, port, reg_ack)
        # if there is no current job & something in queue
        # start a the job at the front of the queue
        if not self.curr_job and len(self.queue) > 0:
            new_job = self.queue[0]
            self.queue.pop(0)
            # update member variables for new job
            self.stage = 'map'
            self.curr_job = new_job
            job_thread = Thread(target=self.execute_job)
            job_thread.start()
            self.threads.append(job_thread)


    def fault(self):
        """Fault tolerance for managers."""
        print('implement me')


    def udp_server(self, host, port):
        """UDP server code."""
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
        for key, _ in self.workers.items():
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
