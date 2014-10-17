from consistent_hash_ring import ConsistentHashRing
import logging
import util
import collections
import pprint
import random
import bisect
import sys

class MembershipStage(object):
    """ Stage for managing ring membeship and failure detection."""

    def __init__(self, server = None,  node_addresses=[], wait_time=30):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._wait_time = wait_time
        self._node_lookup = {util.get_hash(str(node_address)) : node_address for node_address in node_addresses}
        self._consistent_hash_ring = ConsistentHashRing(node_hashes = sorted(self._node_lookup.keys()))

        self.logger.debug('__init__.  node_lookup: {}'.format(self._node_lookup))

        self._failed_to_contact_node_hashes = collections.defaultdict(dict)

        self._processor = self._handle_membership_checks()



    @property
    def node_hashes(self):
        return self._consistent_hash_ring.hash_ring

    def remove_node_hash(self, node_hash):
        success = self._consistent_hash_ring.remove_node_hash(node_hash)
        self._server.num_nodes = len(self.node_hashes)
        return success

    @util.coroutine
    def _handle_membership_checks(self):
        self.logger.debug('_handle_membership_checks')
        next_check_time = util.add_time(util.current_time(), self._wait_time)

        while True:
            # handle failures
            try:
                to_be_removed = []
                for node_hash in self._failed_to_contact_node_hashes:
                    count = self._failed_to_contact_node_hashes[node_hash]['count']
                    if count >= 3:
                        if node_hash in self.node_hashes:
                            self._server.internal_request_stage.handle_unannounced_failure(failure_node_hash=node_hash)
                        to_be_removed.append(node_hash)
                # flush stale contact failures
                for node_hash in self._failed_to_contact_node_hashes:
                    timeout = self._failed_to_contact_node_hashes[node_hash]['timeout']
                    if util.current_time() > timeout:
                        to_be_removed.append(node_hash)

                for node_hash in list(set(to_be_removed)):
                    try:
                        del self._failed_to_contact_node_hashes[node_hash]
                    except:
                        pass

                # retry contacting failure node hashes:
                for node_hash in self._failed_to_contact_node_hashes:
                    if util.current_time() > self._failed_to_contact_node_hashes[node_hash]['timeout']:
                        self._server.internal_request_stage.handle_membership_check(gossip_node_hash=node_hash)

                if util.current_time() > next_check_time:
                    self._server.internal_request_stage.handle_membership_check()
                    next_check_time = util.add_time(util.current_time(), 1)
                    yield
                else:
                    yield
            except Exception as e:
                self.logger.error('_handle_membership_checks error: {}, {}'.format(e, sys.exc_info()))

    def process(self):
        self.logger.debug('process')
        return self._processor.next()

    def report_contact_failure(self, node_hash=None):
        self.logger.debug('report_contact_failure')
        try:
            self._failed_to_contact_node_hashes[node_hash]['count'] += 1
        except:
            self._failed_to_contact_node_hashes[node_hash]['count'] = 1


        try:
            timeout = self._failed_to_contact_node_hashes[node_hash]['timeout']
            new_timeout = util.add_time(timeout, 10)
        except:
            new_timeout = util.add_time(util.current_time(), 10)
        finally:
            self._failed_to_contact_node_hashes[node_hash]['timeout'] = new_timeout

        self._failed_to_contact_node_hashes[node_hash]['next_check_time'] = util.add_time(util.current_time(), 1)

    def node_address(self, node_hash=None):
        """ Returns the  address of a node identified by its hash value in the node ring."""
        if node_hash:
            hostname, _, internal_port = self._node_lookup[node_hash].split(',')
            return hostname, internal_port

    def get_responsible_node_hashes(self, *args, **kwargs):
        """ Returns num_replicas number of node_hashes responsible for the key"""
        self.logger.debug('get_responsible_node_hashes')
        return self._consistent_hash_ring.get_responsible_node_hashes(*args, **kwargs)

    def get_gossip_node_hashes(self, num_gossip_nodes):
        """ Returns num_replicas number of random node_hashes for gossip protocol """
        active_node_hashes = self.node_hashes
        if len(active_node_hashes) >= num_gossip_nodes:
            gossip_node_hashes = random.sample(active_node_hashes, num_gossip_nodes)
        else:
            gossip_node_hashes = random.sample(active_node_hashes, len(active_node_hashes))
        return gossip_node_hashes

    def get_unannounced_failure_repair_node_hashes(self, failure_node_hash=None):
        self.logger.debug('get_unannounced_failure_repair_node_hashes')
        try:
            node_hashes = self.node_hashes
            num_replicas = self._server.num_replicas
            failure_node_hash_index = node_hashes.index(failure_node_hash)
            repair_node_hashes = [node_hashes[i%len(node_hashes)] for i in xrange(failure_node_hash_index-num_replicas+1, failure_node_hash_index+num_replicas)]
            repair_node_hashes.remove(failure_node_hash)
        except:
            pass
        return repair_node_hashes

    def _partition_keys(self):
        """ Returns:
                a dict() where:
                    key: node_hashes
                    value: list of keys for which the given node_hash is responsible
        """
        keys = sorted(self._server.persistence_stage.keys())
        partition = dict()
        left_bound = 0
        for node_hash in  self.node_hashes:
            right_bound = bisect.bisect_left(keys, node_hash)
            partition[node_hash] = keys[left_bound:right_bound]
            left_bound = right_bound
        else:
            partition[self.node_hashes[0]] += keys[right_bound:]

        return partition

    def key_value_partition(self):
        """ Returns:
                a dict() where:
                    key: node_hashes
                    value: dict of {key: values} for which the given node_hash is responsible
        """
        key_partition = self._partition_keys()
        key_value_partition = collections.defaultdict(dict)
        for node_hash in key_partition:
            for key in key_partition[node_hash]:
                reply = self._server.persistence_stage.get(key)
                key_value_partition[node_hash][key] = {'value': reply['value'], 'timestamp': reply['timestamp']}

        return key_value_partition

    def _partition_for_failure(self, node_hash=None):
        if not node_hash:
            node_hash = self._server.node_hash
        index_self = self.node_hashes.index(node_hash)
        num_replicas = self._server.num_replicas
        old_partition = self.key_value_partition()
        new_partition = dict()
        for offset in xrange(1-num_replicas, 1):
            index_old = index_self + offset
            index_new = (index_self + offset + num_replicas) % len(self.node_hashes)

            old_responsible_node_hash = self.node_hashes[index_old]
            new_reponsible_node_hash = self.node_hashes[index_new]
            new_partition[new_reponsible_node_hash] = old_partition[old_responsible_node_hash]

        return new_partition
