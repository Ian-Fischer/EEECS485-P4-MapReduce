"""Utils file.

This file is to house code common between the Manager and the Worker
Socket stuff

"""
import socket
import json
from threading import Thread

def tcp_client(server_host, server_port, msg):
    # create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # connect to the server
        sock.connect((server_host, server_port))
        # send a message
        message = json.dumps(msg)
        sock.sendall(message.encode('utf-8'))

def udp_client(server_host, server_port, worker_host, worker_port):
    """Send worker heartbeats on UDP."""
    heartbeat = {
        "message_type": "heartbeat",
        "worker_host": worker_host,
        "worker_port": worker_port
    }
    while True:
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect((server_host, server_port))
            # Send a message
            message = json.dumps(heartbeat)
            sock.sendall(message.encode('utf-8'))
        Thread.sleep(2)

def tcp_server(host,port,threads = None, manager_host = None, manager_hb_port = None):
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
            if message_dict["message_type"] == "register_ack":
                hb_thread = Thread(target=udp_client, args = (manager_host, manager_hb_port))
                threads.append(hb_thread)
                hb_thread.start()
{
  "message_type": "register_ack",
  "worker_host": string,
  "worker_port": int,
}


def udp_server(host, port, workers):
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Bind the UDP socket to the server 
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)
        # No sock.listen() since UDP doesn't establish connections like TCP
        # Receive incoming UDP messages
        while True:
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
            if key not in workers.keys():
                continue
            workers[key]['last_checkin'] = time.time()
            print(message_dict)