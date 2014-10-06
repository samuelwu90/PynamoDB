import bisect

class ConsistentHashRing(object):
    """
        Implements a consistent hash ring, that given a list of nodes, determines the n nodes responsible for holding a given key.

        It is assumed that nodes and keys have been hashed appropriately prior to interaction with hash ring.
    """

    def __init__(self, node_hashes=None):
        if node_hashes:
            self._hash_ring = sorted(node_hashes)
        else:
            self._hash_ring = []

    def __len__(self):
        """ Returns the number of nodes still in hash ring"""
        return len(self._hash_ring)


    def add_node_hash(self, node_hash=None):
        if node_hash:
            bisect.insort(self._hash_ring, node_hash)

    def remove_node_hash(self, node_hash=None):
        if node_hash:
            self._hash_ring.remove(node_hash)

    def get_responsible_node_hashes(self, key_hash=None, num_replicas=3):
        """ Returns:
                n nodes clockwise of given key hash where n = num_replicas

            Example:
                for nodes [ 'A', 'B', 'C', 'D' ] and key 'AA', [ 'B', 'C', 'D' ] would be returned.
        """

        if key_hash:
            primary_position = bisect.bisect_left(self._hash_ring, key_hash)
            return [self._hash_ring[ i % len(self._hash_ring) ] for i in xrange(primary_position, primary_position + num_replicas)]

