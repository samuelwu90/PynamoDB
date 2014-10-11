from consistent_hash_ring import ConsistentHashRing
import logging
import util
import collections
import random

class MembershipStage(object):
    """ Stage for managing ring membeship and failure detection."""

    def __init__(self, server = None,  node_addresses=[]):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._node_lookup = {util.get_hash(str(node_address)) : node_address for node_address in node_addresses}
        self._consistent_hash_ring = ConsistentHashRing(node_hashes = self._node_lookup.keys())

        self._num_gossip_nodes = self._server.num_replicas

        self.logger.debug('__init__.  node_lookup: {}'.format(self._node_lookup))


    @property
    def node_hashes(self):
        return self._consistent_hash_ring.hash_ring

    def get_gossip_node_hashes(self):
        active_node_hashes = self.node_hashes
        num_gossip_nodes = self._num_gossip_nodes
        if len(active_node_hashes) >= num_gossip_nodes:
            gossip_node_hashes = random.sample(active_node_hashes, num_gossip_nodes)
        else:
            gossip_node_hashes = random.sample(active_node_hashes, len(active_node_hashes))
        return gossip_node_hashes

    def node_address(self, node_hash=None):
        """ Returns the  address of a node identified by its hash value in the node ring."""
        if node_hash:
            hostname, _, internal_port = self._node_lookup[node_hash].split(',')
            return hostname, internal_port

    def get_responsible_node_hashes(self, *args, **kwargs):
        self.logger.debug('get_responsible_node_hashes')
        return self._consistent_hash_ring.get_responsible_node_hashes(*args, **kwargs)

    def remove_node_hash(self, node_hash):
        return self._consistent_hash_ring.remove_node_hash(node_hash)

    def process(self):
        pass
