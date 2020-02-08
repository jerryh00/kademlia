#!/usr/bin/env python3

import sys
import os
import unittest
import subprocess
import multiprocessing
import logging

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def check_server(server, server_result):
    while (True):
        server_out = server.stdout.readline().decode("utf8")
        logging.debug(server_out)
        if server_out.find('received in json: {"function"') != -1:
            server_result.put(True)

def check_node(node, node_result):
    while (True):
        node_out = node.stdout.readline().decode("utf8")
        logging.debug(node_out)
        if node_out.find("Got sw version") != -1:
            node_result.put(True)

class FunctionalTest(unittest.TestCase):
    def test_sw_version(self):
        subprocess.run(["cargo", "build"])
        server = subprocess.Popen([get_script_path() + "/../target/debug/server"],
                stdout = subprocess.PIPE, bufsize=0)

        server_result = multiprocessing.Queue()
        p_server = multiprocessing.Process(target=check_server, args=(server, server_result))
        p_server.start()

        num_nodes = 100
        expected_run_time = 1

        node = list(range(0,num_nodes))
        node_result = list(range(0,num_nodes))
        p_node = list(range(0,num_nodes))
        for i in range(0, num_nodes):
            node[i] = subprocess.Popen([get_script_path() + "/../target/debug/node"], stdout=subprocess.PIPE, bufsize=0)
            node_result[i] = multiprocessing.Queue()

            p_node[i] = multiprocessing.Process(target=check_node, args=(node[i],
                node_result[i]))
            p_node[i].start()

        p_server.join(expected_run_time)

        if server_result.empty():
            server_result = False
        else:
            server_result = server_result.get()
        logging.debug('server_result=%s', server_result)
        if p_server.is_alive():
            p_server.terminate()
            p_server.join()

        logging.info("Terminating server")
        server.terminate()

        for i in range(0, num_nodes):
            if node_result[i].empty():
                node_result[i] = False
            else:
                node_result[i] = node_result[i].get()

        for i in range(0, num_nodes):
            logging.debug('i=%d, result=%s', i, node_result[i])
            if p_node[i].is_alive():
                p_node[i].terminate()
                p_node[i].join()

        logging.info("Terminating node")
        for i in range(0, num_nodes):
            node[i].terminate()

        self.assertTrue(server_result and all(node_result))

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner().run(suite)
