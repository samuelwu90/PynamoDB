import asyncore
import getopt
import logging
import sys
import util

from persistence_stage import PersistenceStage
from membership_stage import MembershipStage
from external_request_stage import ExternalRequestStage
from internal_request_stage import InternalRequestStage

logging.basicConfig(filename='pynamo.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
                    )

class PynamoServer(object):
    """
    PynamoDB server that manages request/membership/persistence stages.
        -alternates between asyncore.loop polling of channels and server.process polling of server stages.

    Attributes:
    ----------
    persistence_stage (PersistenceStage):
        implements get/put/delete.
    membership_stage (MembershipStage):
        handles membership info/checks, partitions keyspace upon failure.
    external_request_stage (ExternalRequestStage):
        handles communications with clients.
    internal_request_stage (InternalRequestStage):
        handles communications with other nodes.
    """

    def __init__(self, hostname, external_port, internal_port, public_dns_name, node_addresses, wait_time=30, num_replicas=3):
         """
        Args:
        ----------
        hostname (str):
            address to which listening ports will be bound.
        external_port (str or int):
            listening port for external communications.
        internal_port (str or int):
            listening port for internal communications.
        public_dns_name (str):
            used for determining node hash.
        node_addresses (list of str):
            list of every node in the system in the format 'hostname,external_port,internal_port'
        wait_time(int, optional):
            number of seconds before internal nodes start communicating with each other, defaults to 30s.
        num_replicas(int, optional):
            number of replicas for each key-value pair, defaults to 3.
        """
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.info('__init__')

        self._num_replicas = num_replicas

        self.hostname = hostname
        self.external_port = external_port
        self.internal_port = internal_port

        self._persistence_stage = PersistenceStage(server=self)
        self._membership_stage = MembershipStage(server=self, node_addresses=node_addresses, wait_time=wait_time)
        self._external_request_stage = ExternalRequestStage(server=self, hostname=hostname, external_port=external_port)
        self._internal_request_stage = InternalRequestStage(server=self, hostname=hostname, internal_port=internal_port)

        self._node_hash = util.get_hash("{},{},{}".format(public_dns_name, str(external_port), str(internal_port)))
        self._terminator = "\r\n"

        self._external_shutdown_flag = False
        self._internal_shutdown_flag = False

        self.logger.debug('__init__ complete.')

    @classmethod
    def from_node_list(cls, node_file, self_dns_name, wait_time):
         """
        Constructor from node list with format:
            'public_dns_name, external_port, internal_port'

        Args:
            node_file (str) : path to node file.
            self_dns_name (str) : own public dns name.
            wait_time(int): number of seconds before internal nodes start communicating with each other, defaults to 30s.

        Returns:
            True if successful, False otherwise.
        """
        try:
            node_addresses = []

            with open(node_file, 'r') as f:
                for line in f:
                    node_addresses.append(line.strip())

            for node_address in node_addresses:
                public_dns_name, external_port, internal_port = node_address.split(',')
                if public_dns_name == self_dns_name:
                    print '0.0.0.0', external_port, internal_port
                    server = cls(   hostname='0.0.0.0',
                                 public_dns_name=self_dns_name,
                                 external_port=int(external_port),
                                 internal_port=int(internal_port),
                                 node_addresses=node_addresses,
                                 wait_time=int(wait_time))
                    return server

            return True
        except:
            return False

    def process(self):
        """
        Instructs processors to process requests.
                Called after each asynchronous loop.
        """
        try:
            self.internal_request_stage.process()
        except:
            self.logger.error('internal_request_stage .process() error, {}.'.format(sys.exc_info()))

        try:
            self.external_request_stage.process()
        except:
            self.logger.error('external_request_stage .process() error, {}.'.format(sys.exc_info()))

        try:
            self.membership_stage.process()
        except:
            self.logger.error('membership_stage .process() error, {}.'.format(sys.exc_info()))

    def _immediate_shutdown(self):
        """
        Instructs ExternalRequestStage and InternalRequestStage to immediately stop listening.
        """
        self.logger.debug('_immediate_shutdown')
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

def main(argv):
    try:
      opts, args = getopt.getopt(argv,"hi:d:w:")
    except getopt.GetoptError:
      print 'server.py -i <nodelistfile> -d <public_dns_name> -w <wait_time>'
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print  'server.py -i <nodelistfile> -d <public_dns_name> -w <wait_time>'
         sys.exit()
      elif opt in ("-i"):
         node_file = arg
      elif opt in ("-d"):
         self_dns_name = arg
      elif opt in ("-w"):
        wait_time = int(arg)

    server = PynamoServer.from_node_list(node_file=node_file, self_dns_name=self_dns_name, wait_time=wait_time)

    while True:
        try:
            asyncore.loop(timeout=0.001, count=1)
            server.process()
        except:
            server._immediate_shutdown()

if __name__ == "__main__":
    main(sys.argv[1:])

