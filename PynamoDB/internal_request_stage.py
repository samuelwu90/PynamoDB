class InternalRequestStage(object):

    def __init__(self, server):
class InternalRequestStage(asyncore.dispatcher):
    """ Listens for external connections from other nodes and creates an InternalChannel upon accepting."""

    def __init__(self, server, hostname, internal_port):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._hostname = hostname
        self._internal_port = internal_port
        self._coordinators = []
        self._channels = []

        # socket stuff
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind((hostname, int(internal_port)))
        self.listen(5)

    def handle_accept(self):
        if self.server.is_accepting_internal_requests():
            sock, client_address = self.accept()
            internal_channel = InternalChannel(server=self._server, sock=sock, client_address=client_address)
            self._channels.append(internal_channel)

    def handle_close(self):
        self.logger.info('handle_close')
        for channel in self._channels:
            channel.close_when_done()
        for coordinator in self._coordinators:
            for channel in coordinator._channels:
                channel.close_when_done()
        self.close()
        self.logger.debug('handle_close.  channels and self closed')

    def process(self):
        self.logger.info('process')
        for coordinator in self._coordinators:
            coordinator.process()

    def handle_internal_request(self, request, reply_listener):
        """ Send request to node_hash and report back to listener"""
        self.logger.debug('handle_internal_request.')
        coordinator = InternalRequestCoordinator(server=self._server, request=request, reply_listener=reply_listener)
        self._coordinators.append(coordinator)

    def _immediate_shutdown(self):
        self.handle_close()

        pass
