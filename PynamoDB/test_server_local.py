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
import util
logging.basicConfig(filename='pynamo.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
                    )

class TestLocalServerSingle(unittest.TestCase):
    def setUp(self):
        self.hostname = "localhost"
        self.external_port = 50000
        self.internal_port = 50001
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

    def test_server_client_startup(self):
        pass

    def test_server_immediate_shutdown(self):
        self.server._immediate_shutdown()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with self.assertRaises(socket.error):
            sock.connect((self.hostname, self.external_port))
        with self.assertRaises(socket.error):
            sock.connect((self.hostname, self.internal_port))

    def test_server_node_hash(self):
        server_node_hash = self.server.node_hash
        lookup_node_hash = self.server.membership_stage._node_lookup.keys()[0]
        self.assertEqual(server_node_hash, lookup_node_hash)

    def test_client_put_single(self):
        key = 'key'
        value = 'value'

        asyncore.loop(timeout=0.001, count=5)
        self.client.put(key, value)
        for _ in xrange(5):
            asyncore.loop(timeout=0.001, count=1)
            self.server.process()

        self.assertEqual(self.client.replies[0]['error_code'], '\x00')

    def test_client_put_1000(self):
        """ 1000 put requests to single local server"""
        num_puts = 1000
        asyncore.loop(timeout=0.001, count=5)

        for i in xrange(num_puts):
            key = util.offset_hex(self.server.node_hash, -i)
            value = util.get_hash(key)
            self.client.put(key, value)

        asyncore.loop(timeout=0.001, count=100)
        self.server.process()
        asyncore.loop(timeout=0.001, count=100)

        self.assertEqual(len(self.client.replies), num_puts)

        for index, reply in enumerate(self.client.replies):
            request =  self.client.requests[index]
            value, timestamp = self.server.persistence_stage._persistence_engine.get(util.get_hash(request['key']))
            self.assertEqual(reply['error_code'], '\x00')
            self.assertEqual(value, request['value'])

    def test_client_get_1000(self):
        n = 1000
        asyncore.loop(timeout=0.001, count=5)

        for i in xrange(n):
            key = util.offset_hex(self.server.node_hash, -i)
            value = util.get_hash(key)
            self.client.put(key, value)
            self.client.get(key)

        for _ in xrange(100):
            asyncore.loop(timeout=0.001, count=1)
            self.server.process()

        self.assertEqual(len(self.client.replies), 2 * n)

        for index, reply in enumerate(self.client.replies):
            request = self.client.requests[index]
            if request['command'] == 'put':
                value, timestamp = self.server.persistence_stage._persistence_engine.get(util.get_hash(request['key']))
                self.assertEqual(reply['error_code'], '\x00')
                self.assertEqual(value, request['value'])
            elif request['command'] == 'get':
                put_request = self.client.requests[index-1]
                self.assertEqual(reply['value'], put_request['value'])

    def test_client_get_1000_nonexistent_keys(self):
        n = 1000

        asyncore.loop(timeout=0.001, count=5)

        for i in xrange(n):
            key = util.offset_hex(self.server.node_hash, -i)
            self.client.get(key)

        for _ in xrange(100):
            asyncore.loop(timeout=0.001, count=1)
            self.server.process()

        self.assertEqual(len(self.client.replies), n)

        for reply in self.client.replies:
            self.assertEqual(reply['error_code'], '\x01')

    def test_client_get_1000_nonexistent_keys(self):
        n = 1000

        asyncore.loop(timeout=0.001, count=5)

        for i in xrange(n):
            key = util.offset_hex(self.server.node_hash, -i)
            self.client.get(key)

        for _ in xrange(100):
            asyncore.loop(timeout=0.001, count=1)
            self.server.process()

        self.assertEqual(len(self.client.replies), n)

        for reply in self.client.replies:
            self.assertEqual(reply['error_code'], '\x01')

    def test_client_delete_1000(self):
        n = 1000
        asyncore.loop(timeout=0.001, count=5)

        for i in xrange(n):
            key = util.offset_hex(self.server.node_hash, -i)
            value = util.get_hash(key)
            self.client.put(key, value)
            self.client.delete(key)

        for _ in xrange(100):
            asyncore.loop(timeout=0.001, count=1)
            self.server.process()

        self.assertEqual(len(self.client.replies), 2 * n)

        for index, reply in enumerate(self.client.replies):
            request = self.client.requests[index]
            if request['command'] == 'put':
                self.assertEqual(reply['error_code'], '\x00')
            elif request['command'] == 'delete':
                self.assertEqual(reply['error_code'], '\x00')
                self.assertEqual(self.server.persistence_stage.get(util.get_hash(request['key']))['error_code'], '\x01')

    def test_client_delete_1000_nonexistent_keys(self):
        n = 1000

        asyncore.loop(timeout=0.001, count=5)

        for i in xrange(n):
            key = util.offset_hex(self.server.node_hash, -i)
            self.client.delete(key)

        for _ in xrange(100):
            asyncore.loop(timeout=0.001, count=1)
            self.server.process()

        self.assertEqual(len(self.client.replies), n)

        for reply in self.client.replies:
            self.assertEqual(reply['error_code'], '\x01')


class TestLocalServerFive(unittest.TestCase):

    def setUp(self):
        num_servers = 5
        hostname = "localhost"
        self.node_addresses = []
        for i in num_servers:
            external_port = 50000 + ( i * 2 - 1)
            internal_port = 50000 + ( i * 2 )
            self.node_addresses.append("{}:{}".format(hostname, str(external_port))
            self.server = PynamoServer(hostname, self.external_port, self.internal_port, self.node_addresses, num_replicas=1)

        self.client = PynamoClient(self.hostname, self.external_port)


    def tearDown(self):
        for server in self.servers:
            try:
                self.server._immediate_shutdown()
            except:
                pass
        try:
            self.client._immediate_shutdown()
        except:
            pass

    def test_server_client_startup(self):
        pass

