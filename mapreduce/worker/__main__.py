"""MapReduce framework Worker node."""
import sys
import os
import logging
import json
import time
import socket
from threading import Thread
import hashlib
import subprocess
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
                    hb_thread = Thread(target=self.udp_client)
                    self.threads.append(hb_thread)
                    hb_thread.start()
                    self.ackd = True
                elif msg_dict['message_type'] == 'new_map_task':
                    # once we recieve a new map task, map it
                    self.map_job(msg_dict)
        # now that the worker is dead, join threads
        LOGGER.info('Joining all worker threads')
        for thread in self.threads:
            LOGGER.info(f'Joining thread {thread.name}')
            thread.join()

    def map_job(self, message_dict):
        """In there like swimwear."""
        """
        msg_dict = {
            "message_type": "new_map_task",
            "task_id": int,
            "input_paths": [list of strings],
            "executable": string,
            "output_directory": string,
            "num_partitions": int,
            "worker_host": string,
            "worker_port": int
        }
        """

        executable = message_dict['executable']
        input_paths = message_dict['input_paths']
        # input paths
        for input_path in input_paths:
            with open(input_path, encoding='utf-8') as infile:
                with subprocess.Popen(
                    [executable],
                    stdin=infile,
                    stdout=subprocess.PIPE,
                    universal_newlines=True,
                ) as map_process:
                    for line in map_process.stdout:
                        # split over tab into [key, value]
                        line_list = line.split('\t')
                        key = line_list[0]
                        o_d = message_dict['output_directory']
                        # Add line to correct partition output file
                        hexd = hashlib.md5(key.encode("utf-8")).hexdigest()
                        keyhash = int(hexd, base=16)
                        partition = keyhash % message_dict['num_partitions']
                        task_id = message_dict['task_id']
                        part1 = "maptask{0:0=5d}".format(task_id)
                        part2 = '-part{0:0=5d}'.format(partition)
                        file_path = o_d+'/'+part1+part2
                        with open(file_path, 'a', encoding='utf-8') as file:
                            file.write(line)

        # now we finished writing
        output_paths = []
        for file_name in os.listdir(message_dict['output_directory']):
            output_paths.append(message_dict['output_directory']+'/'+file_name)
        # craft a short but meaningful message
        message = {
            "message_type": "finished",
            "task_id": message_dict['task_id'],
            "output_paths": output_paths,
            "worker_host": self.host,
            "worker_port": self.port
        }
        # send the message to the manager
        tcp_client(self.manager_host, self.manager_port, message)

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
