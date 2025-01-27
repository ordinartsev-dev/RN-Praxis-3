"""
Tests for RN Praxis 3
"""

import multiprocessing
from sys import stderr

import numpy as np
import pytest
import subprocess
import sys
import time
import util
import zmq

zmq.Context()

test_args = None
distributor_valgrind_test_passed = False
debug_tests = False


@pytest.fixture
def program_args(request):
    d_exec = request.config.option.dist_exec
    w_exec = request.config.option.work_exec
    global debug_tests
    debug_tests = request.config.option.debug_test
    # cache = request.config.option.cache_texts
    # cache_dir = request.config.option.cache_dir
    return {"distributor": d_exec, "worker": w_exec}


@pytest.mark.timeout(15)
def test_workers_reachable(program_args):
    global test_args
    test_args = util.init_tests(program_args)

    base_port = test_args["base_port"]

    for num_workers in [1, 2, 4, 8]:
        workers = np.arange(base_port, base_port + num_workers).tolist()

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        # launch worker here
        port_list = [str(x) for x in workers]
        worker_procs = util.start_threaded_workers(test_args["worker"], port_list)

        workers_reached = 0

        test_message = "map\0"
        rip_message = "rip\0"
        context = zmq.Context.instance()
        for w in workers:
            socket = context.socket(zmq.REQ)
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.RCVTIMEO, 500)
            socket.connect("tcp://127.0.0.1:" + str(w))
            socket.send(bytes(test_message, "ascii"))

            message = None
            try:
                message = socket.recv()
            except zmq.ZMQError:
                print("Failed to receive reply from worker " + str(w))
                socket.close()
                continue

            reply = message[:-1].decode("ascii")
            if reply == "":
                workers_reached += 1

            # rip worker
            socket.send(bytes(rip_message, "ascii"))
            try:
                socket.recv()
            except zmq.ZMQError:
                print("Failed to receive rip from worker " + str(w))

            socket.close()

        util.join_workers(worker_procs)

        assert workers_reached == num_workers, f"Failed to reach all workers for {num_workers} worker threads! Expected {num_workers} but reached {workers_reached}."


@pytest.mark.timeout(15)
def test_distributor_message_size():
    word_list = util.get_part_of_word_list(test_args["word_list"], 250)

    simple_delimiters = test_args["simple_delimiters"]

    max_msg_size = 1500
    max_string_length = 50 * max_msg_size

    test_string = util.generate_text_from_word_list(word_list, simple_delimiters, max_string_length)

    filename = test_args["filename_simple"]
    f = open(filename, "w")
    f.write(test_string)
    f.close()

    port = str(test_args["base_port"])

    # kill any zmq procs currently running
    util.kill_zmq_distributor_and_worker()

    proc = util.start_distributor([test_args["distributor"], filename, port])

    context = zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:" + port)

    map_response = "test1" * (int(max_msg_size/5)-1) + "\0"

    message_lengths_received = []

    while True:
        msg = socket.recv()
        message_lengths_received.append(len(msg))
        req_type = msg[0:3].decode("ascii")

        if req_type == "rip":
            socket.send(b"rip\0")
            break
        elif req_type == "map":
            socket.send(map_response.encode("ascii"))
        elif req_type == "red":
            socket.send(b"test1\0")
    socket.close()
    proc.wait()

    for l in message_lengths_received:
        assert l <= max_msg_size, f"Received message size exceeds max message size of {max_msg_size} bytes."


@pytest.mark.timeout(60)
def test_simple_text():
    filename_simple = test_args["filename_simple"]

    f = open(filename_simple, "r")
    simple_text = f.read()
    f.close()

    base_port = test_args["base_port"]

    for num_workers in [1, 2, 4, 8]:
        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        # launch worker and distributor
        worker_procs = util.start_threaded_workers(test_args["worker"], port_list)
        proc_distributor = util.start_distributor([test_args["distributor"], filename_simple] +
                                                  port_list)

        util.join_workers(worker_procs)

        distributor_output, distributor_err = proc_distributor.communicate()
        correct_word_count = util.count_words(simple_text)

        if debug_tests:
            util.create_test_debug_output("test_simple_text", num_workers, correct_word_count, distributor_output)

        assert distributor_output == correct_word_count, f"{num_workers} workers failed simple text test."


@pytest.mark.timeout(60)
def test_complex_text():
    word_list = util.get_part_of_word_list(test_args["word_list"], 2500)
    all_delimiters = test_args["all_delimiters"]
    max_string_length = 2.5e5

    test_string = util.generate_text_from_word_list(word_list, all_delimiters, max_string_length)

    filename = test_args["filename_complex"]

    f = open(filename, "w")
    f.write(test_string)
    f.close()

    base_port = test_args["base_port"]

    for num_workers in [2, 4, 8]:
        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        # launch worker and distributor
        worker_procs = util.start_threaded_workers(test_args["worker"], port_list)
        proc_distributor = util.start_distributor([test_args["distributor"], filename] +
                                                  port_list)

        util.join_workers(worker_procs)

        distributor_output, distributor_err = proc_distributor.communicate()
        correct_word_count = util.count_words(test_string)

        if debug_tests:
            util.create_test_debug_output("test_complex_text", num_workers, correct_word_count, distributor_output)

        assert distributor_output == correct_word_count, f"{num_workers} workers failed complex text test."


@pytest.mark.timeout(90)
def test_book_1():
    filename = test_args["filename_book_1"]
    base_port = test_args["base_port"]
    book_text = test_args["books"][0]

    file_out = open(filename, "wb")
    file_out.write(book_text)
    file_out.close()

    for num_workers in [2, 4, 8]:
        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        # launch worker and distributor
        worker_procs = util.start_threaded_workers(test_args["worker"], port_list)
        proc_distributor = util.start_distributor([test_args["distributor"], filename] +
                                                  port_list)

        util.join_workers(worker_procs)

        distributor_output, distributor_err = proc_distributor.communicate()

        book_text_str = book_text.decode("ascii", errors="ignore")
        correct_word_count = util.count_words(book_text_str)

        if debug_tests:
            util.create_test_debug_output("test_book_1", num_workers, correct_word_count, distributor_output)

        assert distributor_output == correct_word_count, f"{num_workers} workers failed book 1 test."


@pytest.mark.timeout(90)
def test_book_2():
    filename = test_args["filename_book_2"]
    base_port = test_args["base_port"]
    book_text = test_args["books"][1]

    file_out = open(filename, "wb")
    file_out.write(book_text)
    file_out.close()

    for num_workers in [2, 4, 8]:
        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        # launch worker and distributor
        worker_procs = util.start_threaded_workers(test_args["worker"], port_list)
        proc_distributor = util.start_distributor([test_args["distributor"], filename] +
                                                  port_list)

        util.join_workers(worker_procs)

        distributor_output, distributor_err = proc_distributor.communicate()

        book_text_str = book_text.decode("ascii", errors="ignore")
        correct_word_count = util.count_words(book_text_str)

        if debug_tests:
            util.create_test_debug_output("test_book_2", num_workers, correct_word_count, distributor_output)

        assert distributor_output == correct_word_count, f"{num_workers} workers failed book 2 test."


@pytest.mark.timeout(30)
def test_interoperability():
    base_port = test_args["base_port"]

    # test workers first
    for num_workers in [2, 4, 8]:

        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        worker_procs = util.start_threaded_workers(test_args["worker"], port_list)

        context = zmq.Context.instance()

        map_message = "mapInteroperability test. Test uses python distributor.\0"
        map_message_expected_return = "interoperability1test11uses1python1distributor1\0"
        reduce_message_expected_return = "interoperability1test2uses1python1distributor1\0"
        rip_message = "rip\0"

        worker_responses = dict()

        for port in port_list:
            curr_responses = []

            socket = context.socket(zmq.REQ)
            socket.connect("tcp://127.0.0.1:" + port)

            socket.send(bytes(map_message, "ascii"))
            message = socket.recv()
            message = message.decode("ascii")
            curr_responses.append(message)
            # assert message == map_message_expected_return

            socket.send(bytes("red" + map_message_expected_return, "ascii"))
            message = socket.recv()
            message = message.decode("ascii")
            curr_responses.append(message)
            # assert message == reduce_message_expected_return

            socket.send(bytes(rip_message, "ascii"))
            message = socket.recv()
            message = message.decode("ascii")
            curr_responses.append(message)
            # assert message == rip_message

            socket.close()

            worker_responses[port] = curr_responses

        util.join_workers(worker_procs)

        for res in worker_responses.values():
            assert res[0] == map_message_expected_return
            assert res[1] == reduce_message_expected_return
            assert res[2] == rip_message

    # test distributor second
    port_list = [str(base_port)]

    # kill any zmq procs currently running
    util.kill_zmq_distributor_and_worker()

    context = zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:" + str(base_port))

    map_message = "mapInteroperability test. Test uses python distributor.\0"
    map_message_expected_return = "interoperability1test11uses1python1distributor1\0"
    reduce_message_expected_return = "interoperability1test2uses1python1distributor1\0"
    rip_message = "rip\0"

    distributor_messages = []

    out_filename = test_args["filename_interop"]
    f = open(out_filename, "w")
    f.write(map_message[3:-1])
    f.close()
    proc_distributor = util.start_distributor([test_args["distributor"], out_filename] + port_list)

    message = socket.recv()
    message = message.decode("ascii")
    # print(message)
    # assert message == map_message
    distributor_messages.append(message)
    socket.send(bytes(map_message_expected_return, "ascii"))

    message = socket.recv()
    message = message.decode("ascii")
    # print(message)
    # assert message == "red" + map_message_expected_return
    distributor_messages.append(message)
    socket.send(bytes(reduce_message_expected_return, "ascii"))

    message = socket.recv()
    message = message.decode("ascii")
    # print(message)
    # assert message == rip_message
    distributor_messages.append(message)
    socket.send(bytes(rip_message, "ascii"))

    socket.close()

    distributor_output, distributor_err = proc_distributor.communicate()

    assert distributor_messages[0] == map_message
    assert distributor_messages[1] == "red" + map_message_expected_return
    assert distributor_messages[2] == rip_message

    assert distributor_output == util.count_words(map_message[3:])


@pytest.mark.timeout(60)
def test_load_distribution():
    base_port = test_args["base_port"]

    for num_workers in [2, 4, 8]:
        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        stds = []

        # run test 3 times, because sometimes bad scheduling may affect load distribution
        for i in range(3):
            manager = multiprocessing.Manager()
            return_dict = manager.dict()
            worker_threads = []
            for p in port_list:
                proc = multiprocessing.Process(target=util.run_worker_load_distribution, args=(p, return_dict))
                worker_threads.append(proc)
                proc.start()

            proc_distributor = util.start_distributor([test_args["distributor"], test_args["filename_complex"]] + port_list)

            proc_distributor.wait()
            for t in worker_threads:
                t.join()

            requests_per_worker = return_dict.values()
            curr_std = np.std(requests_per_worker)
            stds.append(curr_std)
            # stop because std is already satisfying condition, no need for extra runs
            if curr_std <= 2:
                break

        std = np.min(stds)
        assert std <= 2


@pytest.mark.timeout(120)
def test_memory_leaks():
    global test_args

    valgrind_output_file = "valgrind_output"
    valgrind_command_base = ["valgrind", "--tool=memcheck", "--xml=yes", "--xml-file=" + valgrind_output_file,
                             "--gen-suppressions=all", "--leak-check=full", "--leak-resolution=med", "--track-origins=yes",
                             "--trace-children=yes", "--vgdb=no", "--fair-sched=yes"]

    valgrind_command_distributor = valgrind_command_base.copy()
    valgrind_command_distributor[3] = "--xml-file=" + valgrind_output_file + "_distributor.xml"

    test_args["filename_valgrind_distributor"] = valgrind_command_distributor[3].split("=")[1]

    valgrind_commands_worker = []
    valgrind_worker_out_files = []
    for i in range(8):
        curr_command = valgrind_command_base.copy()
        curr_filename = valgrind_output_file + "_worker_" + str(i) + ".xml"
        test_args["filename_valgrind_worker" + str(i)] = curr_filename
        valgrind_worker_out_files.append(curr_filename)
        curr_command[3] = "--xml-file=" + curr_filename
        curr_command.append(test_args["worker"])
        valgrind_commands_worker.append(curr_command)

    word_list = util.get_part_of_word_list(test_args["word_list"], 500)
    simple_delimiters = test_args["simple_delimiters"]
    text_length = 100e3

    test_string = util.generate_text_from_word_list(word_list, simple_delimiters, text_length)

    valgrind_test_filename = test_args["filename_valgrind"]
    f = open(valgrind_test_filename, "w")
    f.write(test_string)
    f.close()

    base_port = test_args["base_port"]

    worker_results = []
    distributor_results = []

    for num_workers in [2, 4, 8]:

        workers = np.arange(base_port, base_port + num_workers).tolist()
        port_list = [str(x) for x in workers]

        # kill any zmq procs currently running
        util.kill_zmq_distributor_and_worker()

        # launch worker and distributor
        proc_workers = util.start_threaded_workers(valgrind_commands_worker[:num_workers], port_list)
        proc_distributor = util.start_distributor(valgrind_command_distributor + [test_args["distributor"], valgrind_test_filename] +
                                                  port_list)

        proc_distributor.wait()
        util.join_workers(proc_workers)

        for i in range(num_workers):
            worker_results.append(util.check_valgrind_output_no_errors(valgrind_worker_out_files[i]))
        distributor_results.append(util.check_valgrind_output_no_errors(valgrind_command_distributor[3].split("=")[1]))

    assert int(np.mean(worker_results)) == 1, f"Worker failed to pass valgrind test!"

    global distributor_valgrind_test_passed
    distributor_valgrind_test_passed = int(np.mean(distributor_results)) == 1


@pytest.mark.timeout(3)
def test_valgrind_second_point_and_cleanup():
    time.sleep(1)
    # kill any zmq procs currently running, including memcheck this time
    util.kill_zmq_distributor_and_worker(kill_memcheck=True)

    context = zmq.Context.instance()
    context.destroy()

    if not debug_tests:
        remove_valgrind_xml_files = True
        util.cleanup_files(test_args, remove_valgrind_xml_files)

    assert distributor_valgrind_test_passed, f"Distributor failed to pass valgrind test!"
