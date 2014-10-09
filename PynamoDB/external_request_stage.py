import asynchat
import asyncore
import logging
import util
import socket
import json

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

class ExternalChannel(asynchat.async_chat):
    """ Channel that handles communication with with client."""
    def __init__(self, server=None, sock=None):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        asynchat.async_chat.__init__(self, sock)
        self._server = server
        self._read_buffer = list()
        self._request_queue = list()


        self.set_terminator(self._server.terminator)

    def collect_incoming_data(self, data):
        self.logger.debug('collect_incoming_data.  collected data: {}'.format(data))
        self._read_buffer.append(data)

    def found_terminator(self):
        request = json.loads(''.join(self._read_buffer))
        self._handle_request(request)

        self.logger.debug('found_terminator.  request: {}'.format(request))

    def _send_message(self, message):
        self.push(util.pack_message(message, self._server._terminator))

    def _handle_request(self, request):
        self.logger.debug('_handle_request.')
        request['timestamp'] = util.current_time()
        request['key'] = util.get_hash(request['key'])
        external_request = ExternalRequest(request=request, server=self._server)
        self._request_queue.append(external_request)
    def handle_error(self):
        pass

    def process(self):
        self.logger.info('process')
        if not self._request_queue:
            self.handle_close()

        for external_request in self._request_queue:
            external_request.process()

        for external_request in self._request_queue:
            if external_request.completed:
                self._send_message(external_request._reply)
                self._request_queue.pop(0)
            else:
                break

class ExternalRequest(object):
    """ enables requests to be processed in order."""

    def __init__(self, request=None, server=None):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._reply = None

        self._processor = self._handle_request(request)

    @property
    def completed(self):
        return bool(self._reply)

    @property
    def reply(self):
        return self._reply

    def process(self):
        return self._processor.next()

    @util.coroutine
    def _handle_request(self, request):
        self.logger.info('_handle_request')
        self.logger.debug('_handle_request.  request: {}'.format(request))

        reply_listener = self._reply_listener()
        self._server.internal_request_stage.handle_internal_request(request, reply_listener)
        self.logger.info('_handle_request')
        while True:
            yield self.completed

    @util.coroutine
    def _reply_listener(self):
        self.logger.info('_reply_listener')
        self._reply = (yield)
        self.logger.debug('_reply_listener.  reply received: {}'.format(self._reply))
        yield True





