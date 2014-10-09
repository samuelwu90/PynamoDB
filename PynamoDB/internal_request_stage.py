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

class InternalChannel(asynchat.async_chat):
    """ Handles receiving requests from other nodes """
    def __init__(self, server=None, sock=None, client_address=None, node_hash=None, coordinator_listener=None):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server

        self.set_terminator(self._server.terminator)

        if sock:                # for incoming communication
            self._client_address = client_address
            asynchat.async_chat.__init__(self, sock)

        elif node_hash:     # for outgoing communication
            self._coordinator_listener = coordinator_listener
            node_address = self._server.membership_stage.node_address(node_hash)
            hostname, port = node_address.split(":")
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.connect((hostname, port))
            except:
                self.logger.error('__init__.  failed to connect' )
            asynchat.async_chat.__init__(self)

    def collect_incoming_data(self, data):
        self._read_buffer.append(data)

    def found_terminator(self):
        message = json.loads(''.join(self._read_buffer))
        self._process_incoming_message(message)
        self.close_when_done()

    def _send_message(self, message):
        self.push(util.pack_message(message, self._server._terminator))

    def _process_incoming_message(self, message):
        self.logger.debug('_handle_message.  type: {}'.format(message['type']))
        if messsage['type'] == 'request':
            if request['command'] == 'put':
                reply = self._server.persistence_stage.put(messsage['key'], messsage['value'], messsage['timestamp'])
            elif request['command'] == 'get':
                reply  = self._server.persistence_stage.get(messsage['key'])
            elif request['command'] == 'delete':
                reply = self._server.persistence_stage.delete(messsage['key'])
            self.send_message(reply)
        elif message['type'] == 'reply':
            self._coordinator_listener.send(message)
        elif message['type'] == 'announced failure':
            self._handle_announced_failure_message(message)
        elif message['type'] == 'unannounced failure':
            self._handle_unannounced_failure_message(message)

    def _handle_announced_failure_message(self, message):
        self.logger.debug('_handle_announced_failure')
        # change membership
        # accept any keys
        # propagate message
            #exponential backoff
        pass

    def _handle_unannounced_failure_message(self, message):
        self.logger.debug('_handle_unannounced_failure')
        # change membership
        # accept any keys
        # repartition keys if one of n + 2 replicas affected.
            # pass keys out
        pass

    def handle_error(self):
        self.logger.error('handle_error')

