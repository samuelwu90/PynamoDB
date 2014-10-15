"""
    test_server_local.py
    ~~~~~~~~~~~~
    clear; python -m unittest discover -v
"""

import asyncore
import logging
import random
import socket
import pprint
import sys
import time
import unittest

import util
from server import PynamoServer
from client import PynamoClient

logging.basicConfig(filename='pynamo.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
                    )

class TestLocalServerTen(unittest.TestCase):

    def setUp(self):
        num_servers = 10

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

        self.run_servers()

    def tearDown(self):
        for index, server in enumerate(self.servers):
            try:
                self.servers[index]._immediate_shutdown()
                self.run_servers()
            except:
                pass
        try:
            self.client._immediate_shutdown()
            self.run_servers()
        except:
            pass

        self.run_servers()

    def run_servers(self):
        # try:
        asyncore.loop(timeout=0.001, count=1)
        for index, server in enumerate(self.servers):
            self.servers[index].process()
        # except Exception as e:
        #     print e, sys.exc_info()

    def put_n_times(self, n):
        for _ in xrange(n):
            server = random.choice(self.servers)
            client = PynamoClient(server.hostname, int(server.external_port))

            key = util.get_hash(str(random.random()))
            value = util.get_hash(key)
            client.put(key, value)
            for x in xrange(5):
                self.run_servers()

            client._immediate_shutdown()
            self.run_servers()

    def test_server_client_startup(self):
        pass

    def test_put_single(self):
        """ put (key, value) on random server, run servers, check if value is on correct nodes"""
        server = random.choice(self.servers)
        self.client = PynamoClient(server.hostname, int(server.external_port))

        key = util.offset_hex(server.node_hash, -1)
        value = util.get_hash(key)
        self.client.put(key, value)

        for _ in xrange(5):
            self.run_servers()

        key_hash = util.get_hash(key)
        responsible_node_hashes = server.membership_stage.get_responsible_node_hashes(key_hash)

        for server in self.servers:
            if server.node_hash in responsible_node_hashes:
                self.assertEqual(value, server.persistence_stage.get(key_hash)['value'])

    def test_put_1000(self):
        """ put 1000 (key, value) on random server, run servers, check if value is on correct nodes"""
        n = 100
        for server in self.servers:
            self.client = PynamoClient(server.hostname, int(server.external_port))
            # add an equal number of keys to each server
            for i in xrange(n/len(self.servers)):
                key = util.offset_hex(server.node_hash, - i )
                value = util.get_hash(key)
                self.client.put(key, value)

                # run servers a few times
                for _ in xrange(10):
                    self.run_servers()

                # check key, value pair is on correct replicas
                key_hash = util.get_hash(key)
                responsible_node_hashes = server.membership_stage.get_responsible_node_hashes(key_hash)
                for server in self.servers:
                    if server.node_hash in responsible_node_hashes:
                        self.assertEqual(value, server.persistence_stage.get(key_hash)['value'])

            self.client._immediate_shutdown()

    def test_get_single(self):
        """ put (key, value) on random server, run servers, get key from other random server"""
        server = random.choice(self.servers)
        client = PynamoClient(server.hostname, int(server.external_port))

        key = util.offset_hex(server.node_hash, -1)
        value = util.get_hash(key)
        client.put(key, value)
        for _ in xrange(10):
            self.run_servers()

        put_reply = client.replies.pop(0)
        self.assertEqual(put_reply['error_code'], '\x00')
        client._immediate_shutdown()

        server = random.choice(self.servers)
        client = PynamoClient(server.hostname, int(server.external_port))
        client.get(key)
        for _ in xrange(10):
            self.run_servers()

        get_reply = client.replies.pop(0)
        self.assertEqual(get_reply['error_code'], '\x00')
        self.assertEqual(get_reply['value'], value)
        client._immediate_shutdown()

    def test_get_1000(self):
        """ put random (key, value) on random server, run servers, check if value is on correct nodes"""
        n = 100
        for x in xrange(n):
            server = random.choice(self.servers)
            client = PynamoClient(server.hostname, int(server.external_port))

            key = util.get_hash(str(random.random()))
            value = util.get_hash(key)
            client.put(key, value)
            for _ in xrange(10):
                self.run_servers()

            put_reply = client.replies.pop(0)
            self.assertEqual(put_reply['error_code'], '\x00')
            client._immediate_shutdown()

            server = random.choice(self.servers)
            client = PynamoClient(server.hostname, int(server.external_port))
            client.get(key)
            for _ in xrange(10):
                self.run_servers()

            get_reply = client.replies.pop(0)
            self.assertEqual(get_reply['error_code'], '\x00')
            self.assertEqual(get_reply['value'], value)
            client._immediate_shutdown()

    def test_delete_single(self):
        """ put (key, value) on random server, run servers, delete value, check if value has been deleted"""

        # put (key, value) into hash ring
        server = random.choice(self.servers)
        client = PynamoClient(server.hostname, int(server.external_port))

        key = util.get_hash(str(random.random()))
        value = util.get_hash(key)
        key_hash = util.get_hash(key)
        responsible_node_hashes = server.membership_stage.get_responsible_node_hashes(key_hash)


        client.put(key, value)

        for _ in xrange(5):
            self.run_servers()

        client._immediate_shutdown()

        # delete key from hash ring
        server = random.choice(self.servers)
        client = PynamoClient(server.hostname, int(server.external_port))
        client.delete(key)

        for _ in xrange(5):
            self.run_servers()

        for server in self.servers:
            if server.node_hash in responsible_node_hashes:
                self.assertFalse(server.persistence_stage.get(key_hash)['value'])

        client._immediate_shutdown()

    def test_delete_1000(self):
        """ put (key, value) on random server, run servers, delete value, check if value has been deleted"""
        n=100
        for x in xrange(n):
            # put (key, value) into hash ring
            server = random.choice(self.servers)
            client = PynamoClient(server.hostname, int(server.external_port))

            key = util.get_hash(str(random.random()))
            value = util.get_hash(key)
            key_hash = util.get_hash(key)
            responsible_node_hashes = server.membership_stage.get_responsible_node_hashes(key_hash)

            client.put(key, value)

            for _ in xrange(10):
                self.run_servers()

            client._immediate_shutdown()

            # delete key from hash ring
            server = random.choice(self.servers)
            client = PynamoClient(server.hostname, int(server.external_port))
            client.delete(key)

            for _ in xrange(10):
                self.run_servers()

            for server in self.servers:
                if server.node_hash in responsible_node_hashes:
                    self.assertFalse(server.persistence_stage.get(key_hash)['value'])

            client._immediate_shutdown()

    def test_shutdown_rings_update_via_push(self):
        """ Instruct one server to shut down, check if node hash is present on other servers after running servers."""
        server = random.choice(self.servers)
        self.client = PynamoClient(server.hostname, int(server.external_port))
        shutdown_server_node_hash = server.node_hash

        for server in self.servers:
            self.assertTrue(shutdown_server_node_hash in server.membership_stage.node_hashes)

        self.client.shutdown()

        for _ in xrange(100):
            self.run_servers()

        for server in self.servers:
            self.assertFalse(shutdown_server_node_hash in server.membership_stage.node_hashes)

    def test_shutdown_rings_update_via_gossip(self):
        """ Instruct one server to shut down, check if node hash is present on other servers after running servers."""

        random.shuffle(self.servers)
        server = self.servers.pop()
        server._immediate_shutdown()
        server.process()
        shutdown_server_node_hash = server.node_hash

        for server in self.servers:
            self.assertTrue(shutdown_server_node_hash in server.membership_stage.node_hashes)

        # seed failure information on one of the servers
        self.servers[0].membership_stage.remove_node_hash(shutdown_server_node_hash)

        for _ in xrange(10):
            for __ in xrange(100):
                self.run_servers()
                time.sleep(0.01)

        for server in self.servers:
            self.assertFalse(shutdown_server_node_hash in server.membership_stage.node_hashes)

    def test_shutdown_keys_propagate(self):
        """ Instruct one server to shut down, check if node hash is present on other servers after running servers."""

        n = 100

        for server in self.servers:
            client = PynamoClient(server.hostname, int(server.external_port))
            for i in xrange(n/len(self.servers)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                for _ in xrange(5):
                    self.run_servers()
            client._immediate_shutdown()

        server_down = random.choice(self.servers)
        self.client = PynamoClient(server_down.hostname, int(server_down.external_port))
        self.client.shutdown()

        for _ in xrange(1000):
            self.run_servers()

        for key_hash in server_down.persistence_stage.keys():
            value = server_down.persistence_stage.get(key_hash)['value']
            responsible_node_hashes = server_down.membership_stage.get_responsible_node_hashes(key_hash)
            for responsible_node_hash in responsible_node_hashes:
                for server_up in self.servers:
                    if server_up.node_hash == responsible_node_hash:
                        self.assertEqual(value, server_up.persistence_stage.get(key_hash)['value'])

        # for key in server.persistence_stage.keys():
        #     responsible_node_hashes = server.membership_stage.get_responsible_node_hashes(key_hash)
        #         for server in self.servers:
        #             if server.node_hash in responsible_node_hashes:
        #                 self.assertEqual(value, server.persistence_stage.get(key_hash)['value'])

    def test_unannounced_failure_rings_update_via_gossip(self):
        random.shuffle(self.servers)
        server = self.servers.pop()
        server._immediate_shutdown()
        server.process()
        shutdown_server_node_hash = server.node_hash

        for server in self.servers:
            self.assertTrue(shutdown_server_node_hash in server.membership_stage.node_hashes)

        for _ in xrange(300):
            self.run_servers()
            time.sleep(0.1)

        for server in self.servers:
            self.assertFalse(shutdown_server_node_hash in server.membership_stage.node_hashes)

    def test_unannounced_failure_keys_propagate(self):
        n = 100

        for server in self.servers:
            client = PynamoClient(server.hostname, int(server.external_port))
            for i in xrange(n/len(self.servers)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                for _ in xrange(5):
                    self.run_servers()
            client._immediate_shutdown()

        random.shuffle(self.servers)
        server_down = self.servers.pop()
        server_down._immediate_shutdown()
        server_down.process()
        shutdown_server_node_hash = server_down.node_hash

        for server in self.servers:
            self.assertTrue(shutdown_server_node_hash in server.membership_stage.node_hashes)

        for _ in xrange(300):
            self.run_servers()
            time.sleep(0.1)

        for server in self.servers:
            self.assertFalse(shutdown_server_node_hash in server.membership_stage.node_hashes)

        for key_hash in server_down.persistence_stage.keys():
            value = server_down.persistence_stage.get(key_hash)['value']
            responsible_node_hashes = server_down.membership_stage.get_responsible_node_hashes(key_hash)
            for responsible_node_hash in responsible_node_hashes:
                for server_up in self.servers:
                    if server_up.node_hash == responsible_node_hash:
                        self.assertEqual(value, server_up.persistence_stage.get(key_hash)['value'])



