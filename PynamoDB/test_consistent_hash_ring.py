"""
    test_persistence_stage.py
    ~~~~~~~~~~~~
    Tests that PersistenceEngine's put, get, delete methods raise the correct error codes.

    Run tests with:
    clear; python -m unittest discover -v
"""

import unittest
import consistent_hash_ring
import util


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.hash_ring = consistent_hash_ring.ConsistentHashRing()
        self.key = "key"
        self.value = util.Value("value")

    def tearDown(self):
        pass



