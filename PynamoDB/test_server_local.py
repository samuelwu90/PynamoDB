"""
    test_server_local.py
    ~~~~~~~~~~~~
    clear; python -m unittest discover -v
"""

import asyncore
import unittest
import socket
from server import PynamoServer
from client import PynamoClient
import time
import logging

class TestServerSingle(unittest.TestCase):
    def setUp(self):
        self.hostname = "localhost"
        self.external_port = 57006
        self.internal_port = 57007
        self.node_addresses = [self.hostname + ":" + str(self.external_port)]

        self.server = PynamoServer(self.hostname, self.external_port, self.internal_port, self.node_addresses, num_replicas=1)
        self.client = PynamoClient(self.hostname, self.external_port)


    def tearDown(self):
        try:
            self.server._immediate_shutdown()
        except:
            pass
        try:
            self.client._immediate_shutdown()
        except:
            pass
    def _test_server_client_startup(self):
        pass

    def _test_server_immediate_shutdown(self):
        self.server._immediate_shutdown()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with self.assertRaises(socket.error):
            sock.connect((self.hostname, self.external_port))
        with self.assertRaises(socket.error):
            sock.connect((self.hostname, self.internal_port))


    def _test_server_node_hash(self):
        server_node_hash = self.server.node_hash
        lookup_node_hash = self.server.membership_stage._node_lookup.keys()[0]
        self.assertEqual(server_node_hash, lookup_node_hash)

    def test_client_put(self):
        logging.basicConfig(filename='pynamo_server.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
                    )
        asyncore.loop(timeout=0.01, count=2)
        key = 'key'
        value = 'value'
        self.client.put(key, value)
        asyncore.loop(timeout=0.01, count=5)
        self.server.process()
        asyncore.loop(timeout=0.01, count=5)
        self.assertEqual(self.client._replies[0], {u'error_code': u'\x00'})

        # print self.client._replies