"""MapReduce framework Worker node."""
import sys
import os
import logging
import json
import heapq
import contextlib
import time
import socket
from threading import Thread
import subprocess
import click
from mapreduce.utils import tcp_server, tcp_client, hash_line


# Configure logging
LOGGER = logging.getLogger(__name__)


def map_job_loop(map_process, msg_d, files):
    """Run the loop mapping."""
    with contextlib.ExitStack() as stack:
        task_id = -1
        for line in map_process.stdout:
            # Add line to correct partition output file
            file_p, part, task_id = hash_line(line, msg_d)
            # if the file we need isn't open, open it
            if not files[part]:
                tmp = stack.enter_context(open(file_p, 'a', encoding='utf-8'))
                files[part] = tmp
            # then, write to it
            files[part].write(line)
        return task_id


def reduce_file_sort(input_path):
    """Sort and write to each file for reduce."""
    with contextlib.ExitStack() as stack:
        open_f = stack.enter_context(open(input_path, 'r+', encoding='utf-8'))
        lines = sorted(open_f)
        open_f.truncate(0)
        open_f.seek(0)
        open_f.writelines(lines)
        open_f.close()


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
        self.threads = []

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            sock.settimeout(1)
            # send the register message to the manager
            reg_msg = {
                "message_type": "register",
                "worker_host": host,
                "worker_port": port
            }
            tcp_client(manager_host, manager_port, reg_msg)
            while not self.dead:
                msg_dict = tcp_server(sock)
                # do something with the message
                if msg_dict['message_type'] == 'shutdown':
                    self.dead = True
                # registration message
                elif msg_dict['message_type'] == 'register_ack':
                    # once we get the ack, set up the heartbeat thread
                    hb_thread = Thread(target=self.udp_client,
                                       args=(manager_hb_port,))
                    self.threads.append(hb_thread)
                    hb_thread.start()
                    self.ackd = True
                elif msg_dict['message_type'] == 'new_map_task':
                    # once we recieve a new map task, map it
                    self.map_job(msg_dict)
                elif msg_dict['message_type'] == 'new_reduce_task':
                    # once we recieve a new reduce task, reduce it
                    self.reduce_job(msg_dict)
        # now that the worker is dead, join threads
        for thread in self.threads:
            thread.join()

    def map_job(self, msg_d):
        """In there like swimwear."""
        # input paths
        for input_path in msg_d["input_paths"]:
            with open(input_path, encoding='utf-8') as infile:
                with subprocess.Popen(
                    [msg_d["executable"]],
                    stdin=infile,
                    stdout=subprocess.PIPE,
                    universal_newlines=True,
                ) as map_process:
                    files = []
                    for _ in range(msg_d['num_partitions']):
                        files.append(None)
                    task_id = map_job_loop(map_process, msg_d, files)
        # now we finished writing
        output_paths = []
        for file_name in os.listdir(msg_d['output_directory']):
            # check if maptask{taskid} is in the filename, we did it
            if f'maptask{task_id:0=5d}' in file_name:
                o_p = msg_d['output_directory']+'/'+file_name
                output_paths.append(o_p)
        output_paths.sort()
        # craft a short but meaningful message
        message = {
            "message_type": "finished",
            "task_id": msg_d['task_id'],
            "output_paths": output_paths,
            "worker_host": self.host,
            "worker_port": self.port
        }
        # send the message to the manager
        tcp_client(self.manager_host, self.manager_port, message)

    def reduce_job(self, message_dict):
        """In there like swimwear (part 2)."""
        task_id = message_dict['task_id']
        output_path = message_dict['output_directory']
        output_path = output_path+'/'+f'part-{task_id:0=5d}'
        open_files = []
        for i_path in message_dict['input_paths']:
            reduce_file_sort(i_path)
        with contextlib.ExitStack() as stack:
            for i_path in message_dict['input_paths']:
                file = stack.enter_context(open(i_path, 'r', encoding='utf-8'))
                open_files.append(file)
            with open(output_path, 'a', encoding='utf-8') as outfile:
                with subprocess.Popen(
                    [message_dict['executable']],
                    universal_newlines=True,
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                ) as reduce_process:
                    # Pipe input to reduce_process
                    for line in heapq.merge(*open_files):
                        # run exe on line and write to output
                        reduce_process.stdin.write(line)
            for files in open_files:
                files.close()
        # craft a short but meaningful message
        # now that we are done with that, we finished the lines
        message = {
            "message_type": "finished",
            "task_id": task_id,
            "output_paths": [output_path],
            "worker_host": self.host,
            "worker_port": self.port
        }
        # send the message to the manager
        tcp_client(self.manager_host, self.manager_port, message)

    def udp_client(self, manager_hb_port):
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
                sock.connect((self.manager_host, manager_hb_port))
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
