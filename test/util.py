import multiprocessing
import os
import random
import re
import signal
import string
from io import StringIO
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from collections import Counter
from typing import List, Union

import numpy as np
import pandas as pd
import requests
import shutil
import zmq


def init_tests(args):
    random.seed()

    distributor_exec = args["distributor"]
    worker_exec = args["worker"]
    # cache = args["cache"]
    # cache_dir = args["cache_dir"]

    simple_delimiters = [", ", ". ", " "]
    all_delimiters = [x + " " for x in string.punctuation]

    filename_simple = "test_simple_text.txt"
    filename_complex = "test_complex_text.txt"

    base_port = 6001
    base_port = check_ports_free(base_port)

    word_list_url = "https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt"
    word_list = None
    for i in range(4):
        try:
            url_content = requests.get(word_list_url, timeout=5)
            word_list = url_content.text.splitlines()
            break
        except requests.exceptions.Timeout:
            continue
    if word_list is None:
        sys.exit("Failed to get word list. Can not run tests without it. Quitting...")

    book_base_url = "https://www.gutenberg.org/cache/epub/"
    book_options = ["1513/pg1513.txt", "2701/pg2701.txt", "1342/pg1342.txt", "84/pg84.txt", "145/pg145.txt"]
    book_1 = random.choice(book_options)
    book_options.remove(book_1)
    book_2 = random.choice(book_options)
    book_text_1 = get_book_text(book_base_url + book_1)
    book_list = [book_text_1]
    book_text_2 = get_book_text(book_base_url + book_2)
    book_list.append(book_text_2)

    filename_book_1 = "book_1.txt"
    filename_book_2 = "book_2.txt"

    filename_interop = "interop_test.txt"

    filename_valgrind = "valgrind_test.txt"


    test_args = {"distributor": distributor_exec,
                 "worker": worker_exec,
                 # "cache": cache,
                 # "cache_dir": cache_dir,
                 "simple_delimiters": simple_delimiters,
                 "all_delimiters": all_delimiters,
                 "filename_simple": filename_simple,
                 "filename_complex": filename_complex,
                 "base_port": base_port,
                 "word_list": word_list,
                 "books": book_list,
                 "filename_book_1": filename_book_1,
                 "filename_book_2": filename_book_2,
                 "filename_interop": filename_interop,
                 "filename_valgrind": filename_valgrind,
                 }

    return test_args


# checks if the base port and all following 8 ports are actually free for testing
# if not, check the next 10
def check_ports_free(base_port):
    if not shutil.which("netstat") or sys.platform != "linux":
        print("netstat not found. "
              "To ensure all used ports during testing are free, please install net-tools with: sudo apt install net-tools",
              file=sys.stderr)
        return base_port

    proc = subprocess.Popen(["netstat -tuna | tail -n +2 | tr -s [:blank:] ','"], stdout=subprocess.PIPE, shell=True)
    netstat_output, netstat_err = proc.communicate()

    netstat_output = netstat_output.decode("ascii")
    df = pd.read_csv(StringIO(netstat_output))
    df = df["Local"]
    df = df.apply(lambda x: x.split(":")[-1])

    ports = np.arange(base_port, base_port + 8).tolist()
    port_list = [str(x) for x in ports]

    if sum((df == i).any() for i in port_list) > 0:
        base_port += 10
        base_port = check_ports_free(base_port)
    return base_port


def kill_zmq_distributor_and_worker(kill_memcheck=False):
    try:
        distributor_procs = subprocess.check_output(["pidof", "zmq_distributor"]).decode("utf-8").split()
        distributor_procs = [int(x) for x in distributor_procs]
    except subprocess.CalledProcessError:
        distributor_procs = []
    try:
        worker_procs = subprocess.check_output(["pidof", "zmq_worker"]).decode("utf-8").split()
        worker_procs = [int(x) for x in worker_procs]
    except subprocess.CalledProcessError:
        worker_procs = []
    if kill_memcheck:
        # pgrep required, because pidof only returns exact matches
        try:
            memcheck_procs = subprocess.check_output(["pgrep", "memcheck"]).decode("utf-8").split()
            memcheck_procs = [int(x) for x in memcheck_procs]
        except subprocess.CalledProcessError:
            memcheck_procs = []
    else:
        memcheck_procs = []
    procs = distributor_procs + worker_procs + memcheck_procs
    for p in procs:
        try:
            os.kill(p, signal.SIGKILL)
        except ProcessLookupError:
            continue


def get_part_of_word_list(word_list, n_words):
    word_list_part = random.sample(word_list, n_words)
    return word_list_part


def random_capitalization(words):
    for i in range(len(words)):
        words[i] = ''.join(map(random.choice, zip(words[i].lower(), words[i].upper())))


def generate_text_from_word_list(w_list, delim, text_size):
    word_lengths = [len(x) for x in w_list]
    median_length = np.median(word_lengths)
    words_to_pick = int(text_size / median_length)
    words = random.choices(w_list, k=words_to_pick)
    random_capitalization(words)

    i = random.randint(25,100)
    while i < len(words):
        words.insert(i, "\n")
        i += random.randint(25,100)

    test_string = "".join(x + random.choice(delim) for x in words)

    return test_string

def count_words(input_string):
    input_string = input_string.lower()

    words = re.findall("[a-z]+", input_string)

    words = Counter(words)
    words = sorted(words.items(), key=lambda a: (-a[1], a[0]))

    output_string = "word,frequency\n"

    for w in words:
        output_string += w[0] + "," + str(w[1]) + "\n"

    return output_string

def get_book_text(url):
    book_text = None
    for i in range(4):
        try:
            url_content = requests.get(url, timeout=5)
            book_text = url_content.text.encode(encoding="ascii", errors="ignore")
            break
        except requests.exceptions.Timeout:
            continue
    if book_text is None:
        sys.exit("Failed to get book text. Can not run tests without it. Quitting...")
    return book_text


def create_test_debug_output(test_name, num_w, expected_output, dist_output):
    debug_base_path = "debug/"
    if not os.path.isdir(debug_base_path):
        os.mkdir(debug_base_path)
    out_name = debug_base_path + test_name + "_n-workers=" + str(num_w) + "_"

    f = open(out_name + "expected.txt", "w")
    f.write(expected_output)
    f.close()

    f = open(out_name + "distributor_output.txt", "w")
    f.write(dist_output)
    f.close()


def run_worker(w_exec : Union[str, List[str]], port : str):
    proc_worker = None
    if isinstance(w_exec, str):
        proc_worker = subprocess.Popen([w_exec, port])
    elif isinstance(w_exec, list):
        w_exec.append(port)
        proc_worker = subprocess.Popen(w_exec)
    proc_worker.wait()


def join_workers(workers: list):
    for w in workers:
        if isinstance(w, subprocess.Popen):
            w.wait()
        elif isinstance(w, multiprocessing.Process):
            w.join()


def start_threaded_workers(worker_exec : Union[str, List[List[str]]], port_list : list, use_multiprocessing = True):
    # use_multiprocessing = False
    if use_multiprocessing:
        worker_procs = []
        if isinstance(worker_exec, str):
            for p in port_list:
                proc = multiprocessing.Process(target=run_worker, args=(worker_exec, p))
                worker_procs.append(proc)
                proc.start()
        elif isinstance(worker_exec, list):
            for p, exe in zip(port_list, worker_exec):
                proc = multiprocessing.Process(target=run_worker, args=(exe, p))
                worker_procs.append(proc)
                proc.start()
        return worker_procs
    else:
        proc_workers = None
        if isinstance(worker_exec, str):
            proc_workers = subprocess.Popen([worker_exec] + port_list)
        elif isinstance(worker_exec, list):
            proc_workers = subprocess.Popen(worker_exec[0] + port_list)
        return [proc_workers]


def start_distributor(dist_args : List[str]):
    return subprocess.Popen(dist_args, stdout=subprocess.PIPE, encoding="ascii")


def run_worker_load_distribution(port, return_dict):
    return_message = "test1worker1load1distribution1\0"

    context = zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:" + str(port))

    received_requests = 0

    while True:
        message = socket.recv()
        message = message.decode("ascii")

        msg_type = message[:3]
        if msg_type == "map" or msg_type == "red":
            received_requests += 1
            socket.send(bytes(return_message, encoding="ascii"))
        elif msg_type == "rip":
            socket.send(bytes("rip\0", encoding="ascii"))
            break
        else:
            print("Unknown message type!")
            break
    socket.close()

    return_dict[port] = received_requests


def check_valgrind_output_no_errors(filename):
    if not os.path.isfile(filename):
        return True
    tree = ET.parse(filename)
    errors = tree.findall("error")
    return len(errors) == 0


def cleanup_files(test_args, remove_valgrind_files=True):
    for k, v in test_args.items():
        if not remove_valgrind_files and "valgrind" in k and ".xml" in v:
            continue
        if "filename" in k and os.path.isfile(v):
            os.remove(v)
