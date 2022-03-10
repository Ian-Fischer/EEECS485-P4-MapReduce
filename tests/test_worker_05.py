"""See unit test function docstring."""

import os
import json
from pathlib import Path
import utils
import mapreduce
from utils import TESTDATA_DIR


def manager_message_generator(mock_socket):
    """Fake Manager messages."""
    # Worker register
    utils.wait_for_register_messages(mock_socket)
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 6001,
    }).encode('utf-8')
    yield None

    # Simulate manager creating output directory for worker
    os.mkdir("tmp/test_worker_05/output")

    # New map job
    yield json.dumps({
        "message_type": "new_map_task",
        "task_id": 0,
        "executable": TESTDATA_DIR/"exec/wc_map.sh",
        "input_paths": [
            TESTDATA_DIR/"input/file01",
            TESTDATA_DIR/"input/file02",
        ],
        'output_directory': "tmp/test_worker_05/output",
        "num_partitions": 1,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode('utf-8')
    yield None

    # Wait for worker to finish map job
    utils.wait_for_status_finished_messages(mock_socket)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_two_input_map(mocker):
    """Verify worker correctly completes the map stage with two input files.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.
    """
    utils.create_and_clean_testdir("test_worker_05")

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch("socket.socket")
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = manager_message_generator(mock_socket)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Run student worker code.  When student worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            host="localhost",
            port=6001,
            manager_host="localhost",
            manager_port=6000,
            manager_hb_port=5999,
        )
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the Worker, excluding heartbeat messages
    all_messages = utils.get_messages(mock_socket)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 0,
            "output_paths": [
                "tmp/test_worker_05/output/maptask00000-part00000"
            ],
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]

    # Verify final output
    outfile01 = Path("tmp/test_worker_05/output/maptask00000-part00000")
    with outfile01.open(encoding="utf-8") as infile:
        actual01 = infile.readlines()
    assert sorted(actual01) == sorted([
        "\t1\n",
        "\t1\n",
        "hello\t1\n",
        "hadoop\t1\n",
        "goodbye\t1\n",
        "hadoop\t1\n",
        "hello\t1\n",
        "bye\t1\n",
        "world\t1\n",
        "world\t1\n",
    ])
