import bisect
import logging

class ConsistentHashRing(object):
    """ Implements a consistent hash ring, that given a list of nodes, determines the n nodes responsible for holding a given key.

        It is assumed that nodes and keys have been hashed appropriately prior to interaction with hash ring.
    """

    def __init__(self, node_hashes=None, num_replicas=3):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')
        self.num_replicas = num_replicas

        if node_hashes:
            self._hash_ring = sorted(node_hashes)
        else:
            self._hash_ring = []

    def __len__(self):
        """ Returns the number of nodes still in hash ring"""
        return len(self._hash_ring)

    def add_node_hash(self, node_hash=None):
        """ add node hash to hash ring.

            Returns:
                True if node_hash has already been removed
                False if the node_hash hasn't already been removed
         """
        if node_hash:
            bisect.insort(self._hash_ring, node_hash)

    def remove_node_hash(self, node_hash=None):
        """ remove node from hash ring """
        if node_hash:
            try:
                self._hash_ring.remove(node_hash)
                return True
            except ValueError:
                return False

    @property
    def hash_ring(self):
        return self._hash_ring

    def get_responsible_node_hashes(self, key_hash=None):
        """ Returns:
                n nodes clockwise of given key hash where n = num_replicas

            Example:
                for nodes [ 'A', 'B', 'C', 'D' ] and key 'AA', [ 'B', 'C', 'D' ] would be returned.
        """
        self.logger.debug('get_responsible_node_hashes')
        self.logger.debug('get_responsible_node_hashes.  key_hash, num_replicas: {}, {}'.format(key_hash, self.num_replicas))

        if self.hash_ring and key_hash:
            primary_position = bisect.bisect_left(self.hash_ring, key_hash)
            if len(self.hash_ring) >= self.num_replicas:
                return [self._hash_ring[ (primary_position + i) % len(self._hash_ring) ] for i in xrange(self.num_replicas)]
            else:
                return [self._hash_ring[ (primary_position + i) % len(self._hash_ring) ] for i in xrange(len(self.hash_ring))]
        else:
            return list()
