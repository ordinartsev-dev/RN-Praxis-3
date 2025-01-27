"""
Tests a smaller portion of book_1 to ensure faster execution but the same logic.
"""

import pytest
import subprocess
import sys
import time
import numpy as np
import zmq
import util

test_args = None
debug_tests = False


@pytest.fixture
def program_args(request):
    d_exec = request.config.option.dist_exec
    w_exec = request.config.option.work_exec
    global debug_tests
    debug_tests = request.config.option.debug_test
    return {"distributor": d_exec, "worker": w_exec}


@pytest.mark.timeout(30)
def test_book_small(program_args):
    """
    Similar to test_book_1 but uses only the first 2048 bytes of book_text[0].
    Verifies that the shortened text is processed correctly by the distributor.
    """
    global test_args
    test_args = util.init_tests(program_args)

    base_port = test_args["base_port"]
    book_text_full = test_args["books"][0]

    # Take only the first 2048 bytes for a smaller test
    #small_text = book_text_full[:2048]
    #small_text = book_text_full[:200000]
    small_text = book_text_full


    # We'll reuse the same filename as e.g. "filename_book_small"
    filename_small = test_args["filename_book_1"] + "_small"

    # Write the smaller text to file
    with open(filename_small, "wb") as f:
        f.write(small_text)

    # We'll test for 2 and 4 workers only (you can add 8 if you want)
    for num_workers in [2, 4]:
        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # Kill any zmq procs
        util.kill_zmq_distributor_and_worker()

        # Launch the worker processes
        worker_procs = util.start_threaded_workers(test_args["worker"], port_list)

        # Start distributor
        proc_distributor = util.start_distributor([test_args["distributor"], filename_small] + port_list)

        # Wait for workers, then get distributor output
        util.join_workers(worker_procs)
        distributor_output, distributor_err = proc_distributor.communicate()

        # We decode the small_text as ASCII (ignore errors)
        small_text_str = small_text.decode("ascii", errors="ignore")
        correct_word_count = util.count_words(small_text_str)

        if debug_tests:
            util.create_test_debug_output("test_book_small", num_workers, correct_word_count, distributor_output)

        assert distributor_output == correct_word_count, f"{num_workers} workers failed 'small book' test!"
