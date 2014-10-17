import asynchat
import asyncore
import logging
import util
import socket
import json

class ExternalRequestStage(asyncore.dispatcher):
    """ Listens for external connections from clients and creates an ExternalChannel upon accepting."""

    def __init__(self, server=None, hostname=None, external_port=None):
        print hostname, external_port
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.info('__init__')
        self.logger.debug('__init__.  hostname, external_port: {}, {}'.format(hostname, external_port))

        # socket stuff
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind((hostname, int(external_port)))
        try:
            self.bind((hostname, int(external_port)))
            self.logger.info('__init__.  successfully listening to {}:{}'.format(hostname, external_port))
        except:
            self.logger.error('__init__.  unable to bind to {}:{}'.format(hostname, external_port))
        self.listen(5)

        # protected properties
        self._server = server
        self._hostname = hostname
        self._external_port = external_port
        self._channels = []
        self._coordinators = []

        self.logger.debug('__init__.  Binding to {}:{}'.format(hostname, external_port))

    def handle_accept(self):
        self.logger.debug('handle_accept')
        sock, client_address = self.accept()
        if self._server.is_accepting_external_requests:
            external_channel = ExternalChannel(server=self._server, sock=sock)
            self._channels.append(external_channel)
            self.logger.debug('handle_accept.  accepting connection from: {}'.format(client_address))
        else:
            self.logger.debug('handle_accept.  external shutdown flag: {}.  closing connection.')
            sock.close()

    def handle_close(self):
        self.logger.debug('handle_close')
        for channel in self._channels:
            channel.close_when_done()
        self.close()
        self.logger.debug('handle_close.  channels and self closed')

    def process(self):
        self.logger.debug('process')
        for channel in self._channels:
            channel.process()

    def _immediate_shutdown(self):
        self.logger.debug('_immediate_shutdown')
        self.handle_close()

class ExternalChannel(asynchat.async_chat):
    """ Channel that handles communication with with client."""
    def __init__(self, server=None, sock=None):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        # internal variables
        self._server = server
        self._coordinators = list()
        self._timeout = None

        # async_chat
        asynchat.async_chat.__init__(self, sock)
        self._read_buffer = list()
        self.set_terminator(self._server.terminator)

    def collect_incoming_data(self, data):
        self.logger.info('collect_incoming_data')
        self.logger.debug('collect_incoming_data.  node_hash: {}'.format(self._server.node_hash))
        self.logger.debug('collect_incoming_data.  collected data: {}'.format(data))
        self._read_buffer.append(data)

    def found_terminator(self):
        request = json.loads(''.join(self._read_buffer))
        self.logger.debug('found_terminator.  request: {}'.format(request))
        self._read_buffer = []
        self._process_message(request)

    def _process_message(self, request):
        self.logger.info('_request_handler.')

        request['type'] = 'external request'
        request['timestamp'] = util.current_time()
        if request['command'] != 'shutdown':
            request['key'] = util.get_hash(request['key'])

        coordinator = ExternalRequestCoordinator(server=self._server, request=request)
        self._coordinators.append(coordinator)

        self.logger.debug('_request_handler. coordinator appended: {}'.format(coordinator))

    def _send_message(self, message):
        self.logger.info('_send_message.')
        self.logger.info('_send_message.  message {}'.format(message))
        self.push(util.pack_message(message, self._server._terminator))

        # set timeout to be 30 seconds after last request received
        self._timeout = util.add_time(util.current_time(), 30)

    def process(self):
        """ Processes request queue and returns replies in the correct order"""

        self.logger.info('process')
        # if timeout has been set and it's past the time
        if self._timeout and (util.current_time() > self._timeout):
            self.close_when_done()
            pass

        # process requests
        for coordinator in self._coordinators:
            coordinator.process()

        # send replies if ready
        for index, coordinator in enumerate(self._coordinators):
            if coordinator.completed:
                self._send_message(coordinator._reply)
                self._coordinators.pop(0)
            else:
                break


class ExternalRequestCoordinator(object):
    """ enables requests to be processed in order."""

    def __init__(self, server=None, request=None):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._reply = None

        self._reply_listener = self._reply_listener()
        self._processor = self._request_handler(request)

    @property
    def completed(self):
        return bool(self._reply)

    @property
    def reply(self):
        return self._reply

    def process(self):
        return self._processor.next()

    @util.coroutine
    def _request_handler(self, request):
        self.logger.info('_request_handler')
        self.logger.debug('_request_handler.  request: {}'.format(request))
        self._server.internal_request_stage.handle_internal_message(message=request, reply_listener=self._reply_listener)
        self.logger.debug('_request_handler. handle_internal_message called.')
        while True:
            yield self.completed

    @util.coroutine
    def _reply_listener(self):
        self.logger.info('_reply_listener')
        self._reply = (yield)
        self.logger.debug('_reply_listener.  reply received: {}'.format(self._reply))
        yield True





