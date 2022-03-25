"""See unit test function docstring."""

import filecmp
import multiprocessing
import time
import pytest
import mapreduce
import utils
from utils import TESTDATA_DIR


@pytest.fixture(name="processes")
def processes_setup_teardown():
    """Store list of Manager and Worker processes, then clean up after test.

    The code before the "yield" statement is setup code, which is executed
    before the test.  Code after the "yield" is teardown code, which is
    executed at the end of the test.  Teardown code is executed whether the
    test passed or failed.

    See here to learn more about fixtures:
    https://docs.pytest.org/en/6.2.x/fixture.html
    """
    # Setup code: store a list of processes.  Later, the testcase will append
    # to this list.
    processes = []

    # Transfer control to testcase
    yield processes

    # Teardown code: force all processes to terminate
    for process in processes:
        process.terminate()
        process.join()


def test_grep(processes):
    """Run a grep MapReduce job."""
    utils.create_and_clean_testdir("test_integration_01")

    # Acquire open ports
    manager_port, manager_hb_port, *worker_ports = \
        utils.get_open_port(nports=5)

    # Start Manager
    process = multiprocessing.Process(
        name=f"Manager:{manager_port}",
        target=mapreduce.Manager,
        args=("localhost", manager_port, manager_hb_port)
    )
    process.start()
    processes.append(process)
    utils.wait_for_process_is_ready(process, port=manager_port)

    # Start workers
    for worker_port in worker_ports:
        process = multiprocessing.Process(
            name=f"Worker:{worker_port}",
            target=mapreduce.Worker,
            args=("localhost", worker_port, "localhost", manager_port,
                  manager_hb_port),
        )
        process.start()
        processes.append(process)
        utils.wait_for_process_is_ready(process, port=worker_port)

    # Send new manager job message
    utils.send_message({
        "message_type": "new_manager_job",
        "input_directory": TESTDATA_DIR/"input",
        "output_directory": "tmp/test_integration_01/output",
        "mapper_executable": TESTDATA_DIR/"exec/grep_map.py",
        "reducer_executable": TESTDATA_DIR/"exec/grep_reduce.py",
        "num_mappers": 2,
        "num_reducers": 1
    }, port=manager_port)

    # Wait for output to be created
    utils.wait_for_isfile("tmp/test_integration_01/output/part-00000")

    # Give Worker time to finish writing to the output file
    # HACK: sleep is not a synchronization primitive
    time.sleep(3)

    # Verify final output file contents
    assert filecmp.cmp(
        TESTDATA_DIR/"correct/grep_correct.txt",
        "tmp/test_integration_01/output/part-00000",
        shallow=False
    )

    # Send shutdown message
    utils.send_message({
        "message_type": "shutdown"
    }, port=manager_port)

    # Wait for processes to stop
    utils.wait_for_process_all_stopped(processes)

    # Check for clean exit
    for process in processes:
        assert process.exitcode == 0, \
            "{process.name} exitcode={process.exitcode}"
