from persistence_stage import PersistenceStage
from membership_stage import MembershipStage
from external_request_stage import ExternalRequestStage
from internal_request_stage import InternalRequestStage
import util


class PynamoServer(object):

    def __init__(self, hostname, external_port, internal_port, node_addresses=None, num_replicas=3):
        self._num_replicas = num_replicas

        self._persistence_stage = PersistenceStage(server=self)
        self._membership_stage = MembershipStage(server=self, node_addresses=node_addresses)
        self._external_request_stage = ExternalRequestStage(self, hostname, external_port)
        self._internal_request_stage = InternalRequestStage(self, hostname, internal_port)


        self._node_hash = util.get_hash("{}:{}".format(hostname, external_port))
        self._terminator = "\r\n"

        self._external_shutdown_flag = False
        self._internal_shutdown_flag = False

    def process(self):
        """ Instructs processors to process requests.
            Called after each asynchronous loop.
        """
        self.internal_request_stage.process()
        self.external_request_stage.process()
        self.membership_stage.process()

    def _immediate_shutdown(self):
        self.internal_request_stage._immediate_shutdown()
        self.external_request_stage._immediate_shutdown()


    @property
    def persistence_stage(self):
        return self._persistence_stage

    @property
    def membership_stage(self):
        return self._membership_stage

    @property
    def external_request_stage(self):
        return self._external_request_stage

    @property
    def internal_request_stage(self):
        return self._internal_request_stage

    @property
    def num_replicas(self):
        return self._num_replicas

    @property
    def node_hash(self):
        return self._node_hash

    @property
    def terminator(self):
        return self._terminator

    @property
    def is_accepting_external_requests(self):
        return not self._external_shutdown_flag

    @property
    def is_accepting_internal_requests(self):
        return not self._internal_shutdown_flag





