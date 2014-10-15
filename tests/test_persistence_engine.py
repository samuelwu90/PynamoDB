"""
    test_persistence_engine.py
    ~~~~~~~~~~~~
    Tests PersistenceEngine's put, get, delete methods.  In this case they're a regular Python dict, but for the sake of completeness, I'll include a few tests here.

    Run tests with:
    clear; python -m unittest discover -v
"""

import unittest
import util
from persistence_engine import PersistenceEngine


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.p = PersistenceEngine()
        self.key = util.Key(key="key")
        self.value = util.Value(value="value")

    def tearDown(self):
        pass

    def test_put(self):
        """ Tests put command """
        try:
            self.p.put(self.key, self.value)
        except:
            self.fail()

    def test_get(self):
        """ Tests get command """
        try:
            self.p.put(self.key, self.value)
            value = self.p.get(self.key)
            self.assertEqual(value, self.value)
        except:
            self.fail()

    def test_delete(self):
        """ Tests delete command """
        try:
            self.p.put(self.key, self.value)
            self.p.delete(self.key)
        except:
            self.fail()

        with self.assertRaises(KeyError) as error:
            value = self.p.get(self.key)
