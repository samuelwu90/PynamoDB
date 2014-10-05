from consistent_hash_ring import ConsistentHashRing
from error_code import ErrorCode
import util


def get_hash(value):
    return hashlib.sha256(value).hexdigest()

class MembershipStage(object):
    """ Stage for managing membership."""

    @classmethod
    def from_node_list(cls, file_name, **kwargs):
        node_addresses = []
        with open(file_name) as f:
            for line in f:
                node_addresses.append(line.strip())
        return cls(**kwargs, node_addresses=node_addresses)

    def __init__(self, server = None, node_addresses=[]):
        self._server = server
        self._internal_request_stage = internal_request_stage
        self._node_lookup = {get_hash(node_address) : node_hash for node_address in node_addresses}
        self._consistent_hash_ring = ConsistentHashRing()

    def get_node_address(self, node_hash=None):
        """ Returns the IP address of a node identified by its hash value in the node ring."""
        if node_hash:
            return self._node_lookup[node_hash]

    def partition_keyspace(self):
        """ Returns:
                a dict() where:
                    keys: node_hashes
                    values: keys for which the given node_hash is responsible
        """
        pass
