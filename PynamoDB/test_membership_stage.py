"""
    test_server_local.py
    ~~~~~~~~~~~~
    clear; python -m unittest discover -v
"""

import pprint
import asyncore
import logging
import socket
import random
import time
import unittest

import util
from server import PynamoServer
from client import PynamoClient

logging.basicConfig(filename='pynamo.log',
                    level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
                    )

class TestLocalServerFive(unittest.TestCase):

    def setUp(self):
        num_servers = 6
        hostname = "localhost"
        self.node_addresses = []
        self.servers = []
        for i in xrange(num_servers):
            external_port = 50001 + ( i * 2 - 1)
            internal_port = 50001 + ( i * 2 )
            self.node_addresses.append("{}, {}, {}".format(hostname, str(external_port), str(internal_port)))

        for node_address in self.node_addresses:
            hostname, external_port, internal_port = node_address.split(',')
            self.servers.append(PynamoServer(hostname, int(external_port), int(internal_port), self.node_addresses, num_replicas=3))

        hostname, external_port, _ = random.choice(self.node_addresses).split(',')
        self.run_servers()

    def tearDown(self):
        for index, server in enumerate(self.servers):
            try:
                self.servers[index]._immediate_shutdown()
                asyncore.loop(timeout=0.001, count=1)
            except:
                pass
        try:
            self.client._immediate_shutdown()
            asyncore.loop(timeout=0.001, count=1)
        except:
            pass


        asyncore.loop(timeout=0.001, count=10)

    def run_servers(self):
        asyncore.loop(timeout=0.001, count=1)
        for index, server in enumerate(self.servers):
            self.servers[index].process()
        asyncore.loop(timeout=0.001, count=1)

    def test_remove_node_hash(self):
        server = random.choice(self.servers)
        self.assertTrue(server.node_hash in server.membership_stage.node_hashes)
        server.membership_stage.remove_node_hash(server.node_hash)
        self.assertFalse(server.node_hash in server.membership_stage.node_hashes)

    def test_get_responsible_node_hashes(self):
        server = random.choice(self.servers)
        node_hashes =  sorted(server.membership_stage.node_hashes)
        key_hash = util.offset_hex(server.node_hash, -1)

        # get position of server's node_hash in the node hashes
        index = node_hashes.index(server.node_hash)

        responsible_node_hashes =  set([ node_hashes[ (index + x) % len(node_hashes)] for x in xrange(server.num_replicas) ])
        responsible_node_hashes_from_function = set(server.membership_stage.get_responsible_node_hashes(key_hash))

        self.assertEqual(responsible_node_hashes, responsible_node_hashes_from_function)
