"""
    test_remote.py
    ~~~~~~~~~~~~
    clear; python -m unittest discover -v
"""

import asyncore
import boto.ec2
import logging
import random
import socket
import pprint
import sys
import time
import unittest

import PynamoDB.util as util
from PynamoDB.client import PynamoClient
from scripts import fabfile

logging.basicConfig(filename='./logs/pynamo.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
                    )

class TestRemoteCommands(unittest.TestCase):

    def setUp(self):
        self.external_port = 50000
        self.internal_port = 50001
        self.num_replicas = 3
        self.clients = []

        self.region="us-west-1"
        self.conn = boto.ec2.connect_to_region(self.region)
        self.instances = [instance for reservation in self.conn.get_all_reservations() for instance in reservation.instances]
        self.public_dns_names = [instance.public_dns_name for instance in self.instances]

    def tearDown(self):
        for client in self.clients:
            try:
                client._immediate_shutdown()
            except:
                pass

    def run_client(self, n):
        for _ in xrange(n):
            asyncore.loop(timeout=0.001, count=1)


    def _test_put_single(self):
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))

        self.run_client(300)
        key = util.get_hash(str(random.random()))
        value = util.get_hash(key)
        client.put(key, value)
        self.run_client(300)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()
    def _test_put_1000(self):
        """ Puts 1000 keys."""
        n=1000

        for public_dns_name in self.public_dns_names:
            client=PynamoClient(public_dns_name, int(self.external_port))
            for _ in xrange(n/len(self.public_dns_names)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                self.run_client(10)

            self.run_client(1000)
            self.clients.append(client)

        for client in self.clients:
            self.assertEqual(len(self.client.replies), n/len(self.public_dns_names))
            for reply in client.replies:
                self.assertEqual(reply['error_code'], '\x00')
    def _test_get_single(self):
        """ Puts, then gets key."""
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))

        self.run_client(300)
        key = util.get_hash(str(random.random()))
        value = util.get_hash(key)
        client.put(key, value)
        self.run_client(300)
        for reply in client.replies:
                self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()

        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(300)
        client.get(key)
        self.run_client(300)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')
            self.assertEqual(reply['value'], value)

        client._immediate_shutdown()
    def _test_get_1000(self):
        """
        Puts, then gets n keys.
        -----
        Ran 1 test in 136.385s
        """
        n = 1000

        previously_put_key_values = {}
        for public_dns_name in self.public_dns_names:
            client=PynamoClient(public_dns_name, int(self.external_port))
            for _ in xrange(n/len(self.public_dns_names)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                previously_put_key_values[key] = value
                self.run_client(10)
            self.run_client(1000)
            client._immediate_shutdown()

        time.sleep(10)

        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(1000)

        for key, value in previously_put_key_values.items():
            client.get(key)
            self.run_client(100)

        self.run_client(1000)

        num_correct = 0
        for i, request in enumerate(client.requests):
            key = request['key']
            value = client.replies[i]['value']
            if value == previously_put_key_values[key]:
                num_correct +=1
            else:
                print key, value, previously_put_key_values[key]

        print len(client.requests),  num_correct
        self.assertEqual(num_correct, n)
        client._immediate_shutdown()
    def _test_put_delete_single(self):
        """ Puts, then deletes key."""
        #put
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))

        self.run_client(300)
        key = util.get_hash(str(random.random()))
        value = util.get_hash(key)
        client.put(key, value)
        self.run_client(300)
        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()

        #delete
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(300)
        client.delete(key)
        self.run_client(300)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()
    def _test_put_delete_1000(self):
        """
        Puts, then gets n keys.
        -----
        152.037s
        """
        n = 1000

        previously_put_key_values = {}
        for public_dns_name in self.public_dns_names:
            client=PynamoClient(public_dns_name, int(self.external_port))
            for _ in xrange(n/len(self.public_dns_names)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                previously_put_key_values[key] = value
                self.run_client(10)
            self.run_client(1000)
            client._immediate_shutdown()

        time.sleep(10)

        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(1000)

        for key in previously_put_key_values:
            client.delete(key)
            self.run_client(100)

        self.run_client(1000)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()
    def _test_put_delete_get_single(self):
        """ Puts, deletes, then tries to get deleted key."""
        #put
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))

        self.run_client(300)
        key = util.get_hash(str(random.random()))
        value = util.get_hash(key)
        client.put(key, value)
        self.run_client(300)
        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()

        #delete
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(300)
        client.delete(key)
        self.run_client(300)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()

        #get
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(300)
        client.get(key)
        self.run_client(300)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x01')
            self.assertIsNone(reply['value'])

        client._immediate_shutdown()
    def _test_put_delete_get_1000(self):
        n = 1
        previously_put_key_values = {}

        for public_dns_name in self.public_dns_names:
            client=PynamoClient(public_dns_name, int(self.external_port))
            for _ in xrange(n/len(self.public_dns_names)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                previously_put_key_values[key] = value
                self.run_client(10)
            self.run_client(1000)
            client._immediate_shutdown()

        time.sleep(10)

        # deletes
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(1000)

        for key in previously_put_key_values:
            client.delete(key)
            self.run_client(100)

        self.run_client(1000)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')

        client._immediate_shutdown()


        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))
        self.run_client(1000)

        for key in previously_put_key_values:
            client.delete(key)
            self.run_client(100)

        self.run_client(1000)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x01')
            self.assertIsNone(reply['value'])

        client._immediate_shutdown()


class TestRemoteRollingFailure(unittest.TestCase):

    def setUp(self):
        self.external_port = 50000
        self.internal_port = 50001
        self.clients = []
        self.num_replicas = 3

        self.region="us-west-1"
        self.conn = boto.ec2.connect_to_region(self.region)
        self.instances = [instance for reservation in self.conn.get_all_reservations() for instance in reservation.instances if instance.state in ['running']]
        self.public_dns_names = [instance.public_dns_name for instance in self.instances]

    def tearDown(self):
        for client in self.clients:
            try:
                client._immediate_shutdown()
            except:
                pass

    def run_client(self, n):
        for _ in xrange(n):
            asyncore.loop(timeout=0.001, count=1)

    def _test_shutdown(self):
        hostname=random.choice(self.public_dns_names)
        client=PynamoClient(hostname, int(self.external_port))

        self.run_client(1000)
        client.shutdown()
        self.run_client(1000)

        for reply in client.replies:
            self.assertEqual(reply['error_code'], '\x00')
        client._immediate_shutdown()

        time.sleep(10)

        # with self.assertRaises(socket.error) as context:
        print hostname
        with self.assertRaises(socket.error) as context:
            client=PynamoClient(hostname, int(self.external_port))
            self.run_client(1000)
            client.put("key", "value")
            self.run_client(1000)

    def _test_rolling_failure_announced_shutdown(self):
        n = 10

        previously_put_key_values = {}
        for public_dns_name in self.public_dns_names:
            client=PynamoClient(public_dns_name, int(self.external_port))
            for _ in xrange(n/len(self.public_dns_names)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                previously_put_key_values[key] = value
                self.run_client(100)
            self.run_client(1000)
            client._immediate_shutdown()

        time.sleep(10)

        #rolling failures
        public_dns_names = self.public_dns_names

        for _ in xrange(len(public_dns_names) - self.num_replicas):
            # randomly select one node and pop it
            hostname = random.choice(public_dns_names)
            public_dns_names.remove(hostname)
            print hostname

            # shut down randomly selected node
            client=PynamoClient(hostname, int(self.external_port))
            self.run_client(1000)
            client.shutdown()
            self.run_client(1000)
            client._immediate_shutdown()

            time.sleep(20)

            #randomly select other node and check if all values are properly returned
            hostname = random.choice(public_dns_names)
            client=PynamoClient(hostname, int(self.external_port))
            self.run_client(1000)
            for key, value in previously_put_key_values.items():
                client.get(key)
                self.run_client(200)
            self.run_client(500)

            num_correct = 0
            for i, request in enumerate(client.requests):
                key = request['key']
                value = client.replies[i]['value']
                if value == previously_put_key_values[key]:
                    num_correct +=1

            print n, num_correct
            client._immediate_shutdown()

    def test_rolling_failure_unannounced_termination(self):
        n = 100

        previously_put_key_values = {}
        for public_dns_name in self.public_dns_names:
            client=PynamoClient(public_dns_name, int(self.external_port))
            for _ in xrange(n/len(self.public_dns_names)):
                key = util.get_hash(str(random.random()))
                value = util.get_hash(key)
                client.put(key, value)
                previously_put_key_values[key] = value
                self.run_client(100)
            self.run_client(1000)
            client._immediate_shutdown()

        time.sleep(10)

        #rolling failures
        for _ in xrange(len(self.instances) - self.num_replicas):
            num_correct = 0
            # randomly select one node and pop it
            instance = random.choice(self.instances)
            self.instances.remove(instance)

            instance_dns = instance.public_dns_name
            instance_id = instance.id
            print "terminating: {}".format(instance_dns)

            # shut down randomly selected node
            instance.reboot()

            time.sleep(30)

            #randomly select other node and check if all values are properly returned
            for key, value in previously_put_key_values.items():

                instance_dns = random.choice(self.instances).public_dns_name
                client=PynamoClient(instance_dns, int(self.external_port))
                self.run_client(100)
                client.get(key)
                self.run_client(100)
                time.sleep(5)
                self.run_client(100)
                try:
                    if client.replies[0]['value'] == previously_put_key_values[key]:
                        num_correct +=1
                except:
                    pass

                client._immediate_shutdown()

            print "num_instances, n, num_correct: {}, {}, {}".format(len(self.instances), n, num_correct)


    def _test(self):
        pass
