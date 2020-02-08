#!/usr/bin/env python3

import sys
import os
import subprocess

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def gen_network_for_debug():
    subprocess.run(["cargo", "build"])
    server = subprocess.Popen([get_script_path() + "/../target/debug/server",
        "-t"],
            stdout = subprocess.DEVNULL, bufsize=0)

    num_nodes = 100
    node = list(range(0,num_nodes))
    for i in range(0, num_nodes):
        node[i] = subprocess.Popen([get_script_path() +
            "/../target/debug/node", "-t"], stdout=subprocess.DEVNULL, bufsize=0)

    server.wait()

if __name__ == "__main__":
    gen_network_for_debug()
