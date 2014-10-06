"""
    test_consistent_hash_ring.py
    ~~~~~~~~~~~~
    Tests that PersistenceEngine's put, get, delete methods raise the correct error codes.

    Run tests with:
    clear; python -m unittest discover -v
"""

import unittest
import consistent_hash_ring
import util
import random


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.consistent_hash_ring = consistent_hash_ring.ConsistentHashRing()
        self.node_hash = util.get_hash('value')

    def tearDown(self):
        pass

    def test_add_node_hash(self):
        self.consistent_hash_ring.add_node_hash(self.node_hash)
        self.assertTrue(self.node_hash in self.consistent_hash_ring._hash_ring)

    def test_add_multiple_node_hashes(self):
        for x in xrange(10):
            node_hash = util.get_hash(str(random.random()))
            self.consistent_hash_ring.add_node_hash(node_hash)
        self.assertEqual(self.consistent_hash_ring._hash_ring, sorted(self.consistent_hash_ring._hash_ring))

    def test_remove_node_hash(self):
        self.consistent_hash_ring.add_node_hash(self.node_hash)
        self.consistent_hash_ring.remove_node_hash(self.node_hash)
        self.assertFalse(self.node_hash in self.consistent_hash_ring._hash_ring)

    def test_get_responsible_node_hashes(self):
        node_hashes = ['A', 'B', 'C', 'D', 'E', 'F']
        self.consistent_hash_ring = consistent_hash_ring.ConsistentHashRing(node_hashes)

        responsible_node_hashes =  self.consistent_hash_ring.get_responsible_node_hashes('AA', 3)
        self.assertEqual(responsible_node_hashes, ['B', 'C', 'D'])

    def test_get_responsible_node_hashes_wrap_around(self):
        """ Make sure that the list returns wraps around properly, i.e. 'E, F, A' or 'F, A, B'... """
        node_hashes = ['A', 'B', 'C', 'D', 'E', 'F']
        self.consistent_hash_ring = consistent_hash_ring.ConsistentHashRing(node_hashes)

        responsible_node_hashes =  self.consistent_hash_ring.get_responsible_node_hashes('EE', 3)
        self.assertEqual(responsible_node_hashes, ['F', 'A', 'B'])
