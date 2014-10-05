"""
    test_persistence_stage.py
    ~~~~~~~~~~~~
    Tests that PersistenceEngine's put, get, delete methods raise the correct error codes.

    Run tests with:
    clear; python -m unittest discover -v
"""

import unittest
from persistence_stage import PersistenceStage
import util


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.ps = PersistenceStage()
        self.key = "key"
        self.value = util.Value("value")

    def tearDown(self):
        pass

    def test_put_success(self):
        """ Tests put command """
        try:
            error_code = self.ps.put(self.key, self.value)
            self.assertEqual(error_code, util.ErrorCode('\x00'))
        except:
            self.fail()

    def test_put_new_value(self):
        """ Tests put command """
        try:
            old_value = util.Value("new_value")
            new_value = util.Value("new_value")

            error_code = self.ps.put(self.key, old_value)
            self.assertEqual(error_code, util.ErrorCode('\x00'))
            error_code = self.ps.put(self.key, new_value)
            self.assertEqual(error_code, util.ErrorCode('\x00'))

            error_code, value = self.ps.get(self.key)
            self.assertEqual(value, new_value)
            self.assertEqual(error_code, util.ErrorCode('\x00'))
        except:
            self.fail()

    def test_put_old_value(self):
        """ Tests put command """
        try:
            error_code = self.ps.put(self.key, self.value)
            self.assertEqual(error_code, util.ErrorCode('\x00'))
        except:
            self.fail()

    def test_get_success(self):
        """ Tests get command """
        try:
            self.ps.put(self.key, self.value)
            error_code, value = self.ps.get(self.key)
            self.assertEqual(value, self.value)
            self.assertEqual(error_code, util.ErrorCode('\x00'))
        except:
            self.fail()

    def test_get_inexistant_key(self):
        """ Tests get command on inexistant key"""
        try:
            self.ps.get(self.key)
            error_code, value = self.ps.get(self.key)
            self.assertEqual(value, None)
            self.assertEqual(error_code, util.ErrorCode('\x01'))
        except:
            self.fail()

    def test_delete_success(self):
        """ Tests delete command """
        try:
            self.ps.put(self.key, self.value)
            error_code = self.ps.delete(self.key)
            error_code, value = self.ps.get(self.key)
            self.assertEqual(error_code, util.ErrorCode('\x01'))
        except:
            self.fail()

    def test_delete_inexistant_key(self):
        """ Tests delete command on inexistant key"""
        try:
            error_code = self.ps.delete(self.key)
            self.assertEqual(error_code, util.ErrorCode('\x01'))
        except:
            self.fail()


