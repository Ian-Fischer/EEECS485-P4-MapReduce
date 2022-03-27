"""MapReduce framework Manager node."""
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
        self.job_counter = 0
        self.queue = []
        self.curr_job = {}
        self.curr_job_m = {}
        self.curr_job_r = {}
        self.stage = None
        self.workers = {}

        threads = []
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
        hb_thread = Thread(target=self.udp_server, args=(host, hb_port))
        threads.append(hb_thread)
        hb_thread.start()
        # create tcp socket on given port to call listen
        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # this is a comment
            sock.bind((host, port))
            LOGGER.info('opening the socket')
            sock.listen()
            LOGGER.info('listening, doing timeout next')
            sock.settimeout(1)
            while self.stage != "dead":
                message_dict = tcp_server(sock)
                # do something with the message
                if message_dict['message_type'] == 'shutdown':
                    self.shutdown()
                    self.stage = "dead"
                # registration
                elif message_dict["message_type"] == 'register':
                    self.register_worker(message_dict)
                # new job req.
                elif message_dict['message_type'] == 'new_manager_job':
                    # manager recieves this when recieve a new job
                    self.new_job_direc(message_dict, tmp_path, port)
                elif message_dict['message_type'] == 'finished':
                    # deal with a finished message
                    self.finished(message_dict, port)
        # now that Manager is dead, join all the threads
        LOGGER.info('THREADS joining manager all threads')
        for thread in threads:
            thread.join()
        LOGGER.info('Done joining')

    def finished(self, msg_dict, port):
        """Deal with received finished message."""
        # set task to finished, add output paths to the dictionary
        worker = (msg_dict['worker_host'], msg_dict['worker_port'])
        if self.stage == 'map':
            task_id = msg_dict['task_id']
            output_paths = msg_dict['output_paths']
            self.curr_job_m[task_id]['status'] = 'done'
            self.curr_job_m[task_id]['output_paths'] = output_paths
            self.workers[worker]['status'] = 'ready'
            # if the stage is not finished, assign the task
            if not self.stage_finished():
                # get the next task
                task = self.work()
                if task:
                    self.assign_task(task, worker)
            else:
                LOGGER.info("Manager:%s, end map stage", port)
                self.stage = 'reduce'
                self.start_stage(port)
        # otherwise we are in reduce
        else:
            task_id = msg_dict['task_id']
            output_paths = msg_dict['output_paths']
            self.curr_job_r[task_id]['status'] = 'done'
            self.curr_job_r[task_id]['output_paths'] = output_paths
            self.workers[worker]['status'] = 'ready'
            if not self.stage_finished():
                # get the next task
                task = self.work()
                if task:
                    self.assign_task(task, worker)
            else:
                LOGGER.info("Manager:%s end reduce stage", port)
                self.finish_up()

    def finish_up(self):
        """Finishing up a job for Mr. Manager."""
        if len(self.queue) > 0:
            self.curr_job = self.queue[0]
            self.queue.pop(0)
            # update member variables for new job
            self.stage = 'map'
            self.start_stage()

    def available_workers(self):
        """Check if there are any available workers."""
        for _, worker in self.workers.items():
            if worker['status'] == 'ready':
                return True
        return False

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

    def partition_reduce(self):
        """Partitions the reduce stage."""
        # get the path to the intermediate dir
        reduce_fs = self.curr_job['intermediate']
        num_reducers = self.curr_job['num_reducers']
        # set up the lists
        partitions = []
        for _ in range(self.curr_job['num_reducers']):
            partitions.append([])
        for r_f in os.listdir(reduce_fs):
            # partition is the last five digits of the file name
            partition = int(r_f[-5:])
            # add it!
            partitions[partition].append(str(reduce_fs)+'/'+r_f)
        for partition in partitions:
            partition.sort()
        # now, files are in the right groups, so we need to sort them
        # then, we create the task messages
        for task_id in range(num_reducers):
            self.curr_job_r[task_id] = {
                "status": "no",
                "executable": self.curr_job['reducer_executable'],
                "input_paths": partitions[task_id],
                "output_directory": self.curr_job['output_directory'],
                "worker_host": None,
                "worker_port": None
            }

    def partition_mapper(self):
        """Partition files and create curr_job_m."""
        input_files = os.listdir(self.curr_job['input_directory'])
        input_files.sort()
        num_mappers = self.curr_job['num_mappers']
        # assign the input_file to it's task
        part_files = []
        for _ in range(num_mappers):
            part_files.append([])
        # round-robin
        for i, file_path in enumerate(input_files):
            task_id = i % num_mappers
            path = self.curr_job['input_directory']+'/'+file_path
            part_files[task_id].append(path)
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

    def start_stage(self, port):
        """Execute current job map stage."""
        if self.stage == 'map':
            LOGGER.info("Manager:%s, begin map stage", port)
            # 1. partition the input
            self.partition_mapper()
        else:
            # Start the reduce stage
            # log that we are starting
            LOGGER.info("Manager:%s begin reduce stage", port)
            # in the map stage, we made num_reducers parts
            # so, we should split the files pased of their part #
            # and we will assign those to workers as they come
            # so, first split it into tasks
            self.partition_reduce()
            # now, curr_job_r should be all the task messages
            # then, go through all available workers and assign them tasks
        for key, worker in self.workers.items():
            if worker['status'] == 'ready':
                # send the worker work if there is any
                taskid = self.work()
                if taskid is not None:
                    self.assign_task(taskid=taskid, worker=key)

    def new_job_direc(self, msg_dict, tmp_path, port):
        """Handle a new job request."""
        # make intermediate directory
        intrm_dir_path = tmp_path / f"job-{self.job_counter}" / "intermediate"
        intrm_dir_path.mkdir(parents=True, exist_ok=True)
        msg_dict['intermediate'] = str(intrm_dir_path)
        # get the output directory path
        output_dir_path = pathlib.Path(msg_dict['output_directory'])
        # create the output directory if it does not exist
        output_dir_path.mkdir(parents=True, exist_ok=True)
        # increment job counter
        self.job_counter += 1
        # only execute job if no curr job and there are available workers
        if not self.curr_job and self.available_workers():
            # update member variables
            self.stage = 'map'
            self.curr_job = msg_dict
            self.start_stage(port)
        else:
            self.queue.append(msg_dict)

    def work(self):
        """Check if there are any unassigned partitions in curr_job."""
        if self.stage == 'map':
            partitions = self.curr_job_m
        else:
            partitions = self.curr_job_r
        for taskid, partition in partitions.items():
            if partition['status'] == 'no':
                return taskid
        return None

    def assign_task(self, taskid, worker):
        """Assign a task to a worker."""
        if self.stage == 'map':
            # create the message
            o_d = self.curr_job_m[taskid]['output_directory']
            task_message = {
                "message_type": "new_map_task",
                "task_id": taskid,
                "input_paths": self.curr_job_m[taskid]['input_files'],
                "executable": self.curr_job_m[taskid]['executable'],
                "output_directory": o_d,
                "num_partitions": self.curr_job['num_reducers'],
                "worker_host": worker[0],
                "worker_port": worker[1]
            }
            # send the message to the worker
            tcp_client(worker[0], worker[1], task_message)
            # update the partition for the worker info
            self.curr_job_m[taskid]['worker_host'] = worker[0]
            self.curr_job_m[taskid]['worker_port'] = worker[1]
            self.curr_job_m[taskid]['status'] = 'busy'
            self.workers[worker]['status'] = 'busy'

        elif self.stage == 'reduce':
            # create the message
            o_d = self.curr_job_r[taskid]['output_directory']
            # craft a sensible message
            task_message = {
                "message_type": "new_reduce_task",
                "task_id": taskid,
                "executable": self.curr_job_r[taskid]['executable'],
                "input_paths": self.curr_job_r[taskid]['input_paths'],
                "output_directory": o_d,
                "worker_host": worker[0],
                "worker_port": worker[1]
            }
            # send the message to the worker
            tcp_client(worker[0], worker[1], task_message)
            # update the partition for the worker info
            self.curr_job_r[taskid]['worker_host'] = worker[0]
            self.curr_job_r[taskid]['worker_port'] = worker[1]
            self.curr_job_r[taskid]['status'] = 'busy'
            self.workers[worker]['status'] = 'busy'

    def register_worker(self, msg_dict):
        """Add worker to manager's worker dict."""
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
        # if there is a job and work to be done, assign
        if self.curr_job:
            # check to see if there is a task
            task = self.work()
            # if there is, assign it to the worker
            if task:
                self.assign_task(task, (host, port))
        # if there is no current job & something in queue
        # start a the job at the front of the queue
        if not self.curr_job and len(self.queue) > 0:
            self.curr_job = self.queue[0]
            self.queue.pop(0)
            # update member variables for new job
            self.stage = 'map'
            self.start_stage()

    def fault(self, host_port, worker):
        """Fault tolerance for managers."""
        if worker['status'] == 'busy':
            if self.stage == 'map':
                job = self.curr_job_m
            else:
                job = self.curr_job_r
            taskid = None
            for key, partition in job.items():
                p_host = partition['worker_host']
                p_port = partition['worker_port']
                w_host, w_port = host_port[0], host_port[1]
                if p_host == w_host and p_port == w_port:
                    partition['status'] = 'no'
                    taskid = key
            # check for available workers in case
            # all other jobs are done
            busy = 0
            for val in self.workers.values():
                busy += val['status'] == 'busy'
            # if this was the only worker doing anything
            if busy == 1:
                # then reassign the work
                for key, val in self.workers.items():
                    if val['status'] == 'ready':
                        self.assign_task(taskid, key)
                        break

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
            while self.stage != "dead":
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
                key = (message_dict['worker_host'],
                       message_dict['worker_port'])
                # ignore if not registered
                if key not in self.workers:
                    continue
                self.workers[key]['last_checkin'] = time.time()
                # update worker times and check for death after message
                curr_time = time.time()
                for host_port, worker in self.workers.items():
                    if worker['status'] != 'dead':
                        if curr_time - worker['last_checkin'] > 12:
                            self.fault(host_port, worker)
                            worker['status'] = 'dead'

    def shutdown(self):
        """Shutdown manager."""
        # send shutdown to all registered workers
        msg = {
            'message_type': 'shutdown'
        }
        for key, worker in self.workers.items():
            if worker['status'] != 'dead':
                # get workers host and port
                server_host, server_port = key[0], key[1]
                # send the message
                tcp_client(server_host, server_port, msg)
                worker['status'] = 'dead'
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
