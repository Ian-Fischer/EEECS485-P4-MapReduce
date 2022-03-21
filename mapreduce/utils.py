"""Utils file.

This file is to house code common between the Manager and the Worker
Socket stuff

"""
import socket
import json
import logging

LOGGER = logging.getLogger(__name__)


def tcp_client(server_host, server_port, msg):
    """Send a message to server_host at server_port."""
    # create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # connect to the server
        sock.connect((server_host, server_port))
        # send a message
        message = json.dumps(msg)
        sock.sendall(message.encode('utf-8'))


def tcp_server(sock):
    """Runs the infinite listen."""
    while True:
        # Wait for a connection for 1s.  The socket library avoids consuming
        # CPU while waiting for a connection.
        try:
            clientsocket, _ = sock.accept()
        except socket.timeout:
            continue
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
        return message_dict
