"""See unit test function docstring."""

import os
import json
import shutil
from pathlib import Path
import utils
import mapreduce
from utils import TESTDATA_DIR


def manager_message_generator(mock_socket):
    """Fake Manager messages."""
    # Worker Register
    utils.wait_for_register_messages(mock_socket)
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 6001,
    }).encode('utf-8')
    yield None

    # Simulate manager creating output directory for worker
    os.mkdir("tmp/test_worker_07/output")

    # Reduce task
    yield json.dumps({
        "message_type": "new_reduce_task",
        "task_id": 0,
        "executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "input_paths": [
            "tmp/test_worker_07/maptask00000-part00000",
            "tmp/test_worker_07/maptask00001-part00000",
        ],
        "output_directory": "tmp/test_worker_07/output",
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for worker to finish reduce job
    utils.wait_for_status_finished_messages(mock_socket)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_reduce_two_inputs(mocker):
    """Verify worker correctly completes a reduce task with two input files.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.
    """
    utils.create_and_clean_testdir("test_worker_07")
    shutil.copyfile(
        TESTDATA_DIR/"test_worker_07/maptask00000-part00000",
        "tmp/test_worker_07/maptask00000-part00000",
    )
    shutil.copyfile(
        TESTDATA_DIR/"test_worker_07/maptask00001-part00000",
        "tmp/test_worker_07/maptask00001-part00000",
    )

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

    # Verify messages sent by the Worker
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
                "tmp/test_worker_07/output/part-00000"
            ],
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]

    # Verify reduce stage output
    with Path("tmp/test_worker_07/output/part-00000").open(encoding="utf-8") \
         as infile:
        reduceout = infile.readlines()
    assert reduceout == [
        "\t2\n",
        "bye\t1\n",
        "hello\t2\n"
    ]
