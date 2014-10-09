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

class InternalRequestCoordinator(object):
    def __init__(self, server=None, request=None, reply_listener=None, timeout=1, max_retries=3):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._request = request
        self._reply_listener = reply_listener

        # for communications
        self._reply = None
        self._replies = dict()
        self._channels = list()

        # for timeouts
        self._timeout = util.add_time(util.current_time(), timeout)
        self._retries = dict()
        self._max_retries = max_retries

        #listener and processor
        self._coordinator_listener = self._listener()
        self._processor = self._handle_request(request=request, reply_listener=reply_listener)



    def process(self):
        """ returns:
                True if completed
                False if still handling
        """
        return self._processor.next()

    @property
    def timed_out(self):
        return util.current_time() > self._timeout

    @property
    def complete(self):
        return self._reply

    @util.coroutine
    def _handle_request(self, request, reply_listener):
        self.logger.debug('_handle_request')

        if request['command'] in ['put', 'delete', 'shutdown']:
            reply = {'error_code' : '\x00'}
            reply_listener.send(reply)

        responsible_node_hashes = self._server.membership_stage.get_responsible_node_hashes(request['key'], num_replicas=self._server.num_replicas)
        self.logger.debug('_handle_request.  key, node_hashes: {}, {}'.format(request['key'], responsible_node_hashes))

        for node_hash in responsible_node_hashes:
            self._replies[node_hash] = None
            self._retries[node_hash] = 0

            if node_hash == self._server.node_hash:
                self._handle_request_locally(request)
            else:
                self._handle_request_remotely(request)

        while True:
            if self.timed_out and not self.complete:
                self._handle_timeout()
            else:
                yield self.complete

    def _handle_request_locally(self, request):
        self.logger.debug('_handle_request_locally')
        if request['command'] == 'put':
            reply = self._server.persistence_stage.put(request['key'], request['value'], request['timestamp'])
        elif request['command'] == 'get':
            reply = self._server.persistence_stage.put(request['key'], request['value'])
        elif request['command'] == 'delete':
            reply = self._server.persistence_stage.put(request['key'], request['value'])

        self.logger.debug('_handle_request_locally.  sending reply: {}'.format(reply))
        self._coordinator_listener.send(reply)

    def _handle_request_remotely(self, request, node_hash):
        self.logger.debug('_handle_request_remotely')
        try:
            internal_channel = self.InternalChannel(coordinator_listener=self._coordinator_listener)
            internal_channel._send_message(request)
            self._channels.append(internal_channel)
        except:
            pass

    def _handle_timeout(self):
        """  Retries requests up to _max_num_tries, then considers
        """
        for node_hash, reply in self._replies:
            if not reply:
                if ( self._retries[node_hash] < self._max_retries ):
                    self._retries[node_hash] += 1
                    self._handle_request_remotely(self._request, node_hash)
                else:
                    self._server._handle_unannounced_failure(node_hash)
            else:
                self._coordinator_listener.send(reply)

    @util.coroutine
    def _listener(self):
        self.logger.debug('_listener')

        for _ in xrange(self._server.num_replicas):
            reply = (yield)
            self.logger.debug('_listener. reply received: {}'.format(reply))
            self._replies[reply['node_hash']] = reply

        self._process_replies()

        while True:
            yield True

    def _process_replies(self):
        """ Process n replies.
                If the request is get, return the most recent value and perform repairs if necessary.
        """

        self.logger.debug('_process_replies')

        if self._request['command'] == 'get':
            replies = self._replies.values().sort(self._replies, key=lambda reply: reply['timestamp'])
            newest_reply = replies[0]
            self._reply_listener.send(newest_reply)

            for node_hash, reply in self._replies:
                if reply['value'] != newest_value:
                    pass # repair at read
        else:
            reply = self._replies.values()[0]
            self.logger.debug('_process_replies.  reply: {}'.format(reply))
