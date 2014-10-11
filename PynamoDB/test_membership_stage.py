"""
    test_server_local.py
    ~~~~~~~~~~~~
    clear; python -m unittest discover -v
"""

import bisect
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
        self.server_lookup = {}

        for i in xrange(num_servers):
            external_port = 50001 + ( i * 2 - 1)
            internal_port = 50001 + ( i * 2 )
            self.node_addresses.append("{}, {}, {}".format(hostname, str(external_port), str(internal_port)))

        for node_address in self.node_addresses:
            hostname, external_port, internal_port = node_address.split(',')
            server = PynamoServer(hostname, int(external_port), int(internal_port), self.node_addresses, num_replicas=3)
            self.servers.append(server)
            self.server_lookup[server.node_hash] = server

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

    def test_remove_node_hash(self):
        """ check that function removes node_hash from consistent hash ring """
        server = random.choice(self.servers)
        self.assertTrue(server.node_hash in server.membership_stage.node_hashes)
        server.membership_stage.remove_node_hash(server.node_hash)
        self.assertFalse(server.node_hash in server.membership_stage.node_hashes)

    def test_get_responsible_node_hashes(self):
        """ check function returns three successor node_hashes for key """
        server = random.choice(self.servers)
        node_hashes =  sorted(server.membership_stage.node_hashes)
        key_hash = util.offset_hex(server.node_hash, -1)

        # get position of server's node_hash in the node hashes
        index = node_hashes.index(server.node_hash)

        responsible_node_hashes =  set([ node_hashes[ (index + x) % len(node_hashes)] for x in xrange(server.num_replicas) ])
        responsible_node_hashes_from_function = set(server.membership_stage.get_responsible_node_hashes(key_hash))

        self.assertEqual(responsible_node_hashes, responsible_node_hashes_from_function)

    def test_partition_keys(self):
        """ put n keys on servers, get partition, check that each key in partition is assigned to its primary responsible node"""

        self.put_n_times(1000)

        server = random.choice(self.servers)
        node_hashes = server.membership_stage.node_hashes
        partition = server.membership_stage.partition_keys()

        # ensure only a num_replica number of partitions are present on a node
        self.assertTrue(sum(bool(value) for value in partition.values()) <= server.num_replicas)

        # ensure each key is assigned to its primary responsible node hash
        for node_hash in partition:
            for key in partition[node_hash]:
                responsible_node_hash = node_hashes[bisect.bisect_left(node_hashes, key) % len(node_hashes)]
                self.assertTrue(node_hash, responsible_node_hash)

    def test_partition_keys_for_announced_failure(self):
        """ put n keys on servers, get reassignment partition, check keys not already on nodes, nodes actually responsible for key"""

        self.put_n_times(1000)

        server = random.choice(self.servers)
        new_partition = server.membership_stage._partition_keys_for_announced_failure()
        server.membership_stage.remove_node_hash(server.node_hash)

        for node_hash in new_partition:
            for key_hash in new_partition[node_hash]:
                # ensure each node_hash in new_partition doesn't already contain the key
                self.assertFalse(self.server_lookup[node_hash].persistence_stage.get(key_hash)['value'])
                # ensure each node_hash is actually responsible for the key with server's node_hash removed
                self.assertTrue(node_hash in server.membership_stage.get_responsible_node_hashes(key_hash))
