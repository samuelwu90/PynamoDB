from persistence_stage import PersistenceStage
from membership_stage import MembershipStage
from external_request_stage import ExternalRequestStage
from internal_request_stage import InternalRequestStage
import asyncore
import util
import logging
import sys
import getopt

logging.basicConfig(filename='./logs/pynamo.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
                    )

class PynamoServer(object):

    def __init__(self, hostname, external_port, internal_port, node_addresses=None, wait_time=1, num_replicas=3):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.info('__init__')

        self._num_replicas = num_replicas
        self._num_nodes = len(node_addresses)

        self.hostname = hostname
        self.external_port = external_port
        self.internal_port = internal_port

        self._persistence_stage = PersistenceStage(server=self)
        self._membership_stage = MembershipStage(server=self, node_addresses=node_addresses, wait_time=wait_time)
        self._external_request_stage = ExternalRequestStage(server=self, hostname=hostname, external_port=external_port)
        self._internal_request_stage = InternalRequestStage(server=self, hostname=hostname, internal_port=internal_port)


        self._node_hash = util.get_hash("{},{},{}".format(hostname, str(external_port), str(internal_port)))
        self._terminator = "\r\n"

        self._external_shutdown_flag = False
        self._internal_shutdown_flag = False

        self.logger.debug('__init__ complete.')

    @classmethod
    def from_node_list(cls, node_file, self_dns_name):
        node_addresses = []

        with open(node_file, 'r') as f:
            for line in f:
                print line
                node_addresses.append(line.strip())

        for node_address in node_addresses:
            public_dns_name, external_port, internal_port = node_address.split(',')
            if public_dns_name == self_dns_name:
                server = cls(   hostname=public_dns_name,
                             external_port=int(external_port),
                             internal_port=int(internal_port),
                             node_addresses=node_addresses,
                             wait_time=30)
                return server

        return None

    def process(self):
        """ Instructs processors to process requests.
            Called after each asynchronous loop.
        """
        try:
            self.internal_request_stage.process()
        except:
            self.logger.error('internal_request_stage .process() error.')

        try:
            self.external_request_stage.process()
        except:
            self.logger.error('external_request_stage .process() error.')

        try:
            self.membership_stage.process()
        except:
            self.logger.error('membership_stage .process() error.')

    def _immediate_shutdown(self):
        self.logger.info('_immediate_shutdown')
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
      opts, args = getopt.getopt(argv,"hi:d:")
    except getopt.GetoptError:
      print 'server.py -i <nodelistfile> -d <public_dns_name>'
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print  'server.py -i <nodelistfile> -d <public_dns_name>'
         sys.exit()
      elif opt in ("-i"):
         node_file = arg
         print node_file
      elif opt in ("-d"):
         self_dns_name = arg
         print self_dns_name

    print "Starting server with node_file and dns_name: {}, {}".format(node_file, self_dns_name)
    server = PynamoServer.from_node_list(node_file=node_file, self_dns_name=self_dns_name)
    print "Starting server with node_file and dns_name: {}, {}".format(node_file, self_dns_name)

    while True:
        asyncore.loop(timeout=0.001, count=1)
        server.process()

if __name__ == "__main__":
    main(sys.argv[1:])

