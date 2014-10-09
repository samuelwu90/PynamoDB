class ExternalRequestStage(object):

    def __init__(self, server):
class ExternalRequestStage(asyncore.dispatcher):
    """ Listens for external connections from clients and creates an ExternalChannel upon accepting."""

    def __init__(self, server, hostname, external_port):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        # socket stuff
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind((hostname, int(external_port)))
        self.listen(5)

        # protected properties
        self._server = server
        self._hostname = hostname
        self._external_port = external_port
        self._channels = []

        self.logger.debug('__init__.  Binding to {}:{}'.format(hostname, external_port))


    def handle_accept(self):
        self.logger.debug('handle_accept')
        if self._server.is_accepting_external_requests:
            sock, client_address = self.accept()
            external_channel = ExternalChannel(server=self._server, sock=sock)
            self._channels.append(external_channel)

            self.logger.debug('handle_accept.  accepting connection from: {}'.format(client_address))


    def handle_close(self):
        self.logger.debug('handle_close')
        for channel in self._channels:
            channel.close_when_done()
        self.close()

        self.logger.debug('handle_close.  channels and self closed')

    def process(self):
        self.logger.debug('process')
        for external_channel in self._channels:
            external_channel.process()

    def _immediate_shutdown(self):
        self.logger.debug('_immediate_shutdown')
        self.handle_close()

        pass
