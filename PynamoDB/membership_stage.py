from consistent_hash_ring import ConsistentHashRing
import logging
import util
import collections

class MembershipStage(object):
    """ Stage for managing ring membeship and failure detection."""

    def __init__(self, server = None,  node_addresses=[], num_gossip_nodes=3):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._node_lookup = {util.get_hash(str(node_address)) : node_address for node_address in node_addresses}
        self._consistent_hash_ring = ConsistentHashRing(node_hashes = self._node_lookup.keys())

        self._num_gossip_nodes = num_gossip_nodes

        self.logger.debug('__init__.  node_lookup: {}'.format(self._node_lookup))


    @property
    def node_hashes(self):
        return self._consistent_hash_ring._node_hashes.hash_ring

    def node_address(self, node_hash=None):
        """ Returns the  address of a node identified by its hash value in the node ring."""
        if node_hash:
            return self._node_lookup[node_hash]

    def get_responsible_node_hashes(self, *args, **kwargs):
        self.logger.debug('get_responsible_node_hashes')
        return self._consistent_hash_ring.get_responsible_node_hashes(*args, **kwargs)

    def remove_node_hash(self, node_hash):
        return self._consistent_hash_ring.remove_node_hash(node_hash)

    def process(self):
        pass
