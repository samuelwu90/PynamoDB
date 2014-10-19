import asynchat
import asyncore
import logging
import util
import socket
import json

class ExternalRequestStage(asyncore.dispatcher):
    """
    Listens for external connections from clients and routes requests internally.
    """

    def __init__(self, server=None, external_port=None, hostname="0.0.0.0"):
        """
        Parameters
        ----------
        server : PynamoServer object.
            object through which internal stages can be accessed.
        external_port : str or int
            port on which server will be listening for external communications.
        hostname : str
            public dns name through which server will be contacted.
        """
        print hostname, external_port
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')
        self.logger.debug('__init__.  hostname, external_port: {}, {}'.format(hostname, external_port))

        # socket stuff
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.bind((hostname, int(external_port)))
            self.logger.info('__init__.  successfully listening to {}:{}'.format(hostname, external_port))
        except:
            self.logger.error('__init__.  unable to bind to {}:{}'.format(hostname, external_port))
        self.listen(5)

        # protected properties
        self._server = server
        self._hostname = hostname
        self._external_port = int(external_port)
        self._channels = []
        self._coordinators = []
        self._processor = self._request_handler()

    def handle_accept(self):
        """
        Implements asyncore.dispatcher's handle_accept method.
        """
        self.logger.info('handle_accept')
        sock, client_address = self.accept()
        if self._server.is_accepting_external_requests:
            external_channel = ExternalChannel(server=self._server, sock=sock)
            self._channels.append(external_channel)
            self.logger.info('handle_accept.  accepting connection from: {}'.format(client_address))
        else:
            self.logger.debug('handle_accept.  external shutdown flag: {}.  closing connection.'.format(self_server._external_shutdown_flag))
            sock.close()

    def handle_close(self):
        """
        Implements asyncore.dispatcher's handle_accept method.

        Closes each asynchat channel and then closes self.
        """
        self.logger.debug('handle_close')
        for channel in self._channels:
            channel.close_when_done()
        self.close()
        self.logger.info('handle_close.  channels and self closed')

    def process(self):
        """
        Signals coroutine to cycle through its next loop.
        """
        self.logger.debug('process')
        return self._processor.next()

    @util.coroutine
    def _request_handler(self):
        """
        Coroutine that cycles through self._channels and processes each one in turn.
        """

        yield   # do nothing when first called

        while True:

            # handle shtu
            if self._server._external_shutdown_flag:
                if not self._channels:
                    self.handle_close()
                    yield False

            to_be_removed = []
            for channel in self._channels:
                if channel.process():
                    to_be_removed.append(channel)

            yield True

    def _immediate_shutdown(self):
        """
        Stops self from listening on self._external_port immediately.
        """
        self.logger.debug('_immediate_shutdown')
        self.handle_close()

class ExternalChannel(asynchat.async_chat):
    """
    asyncore channel that handles external communication with a client.
    """
    def __init__(self, server=None, sock=None):
        """
        Parameters
        ----------
        server : PynamoServer
            object through which internal stages can be accessed.
        sock : socket
            socket connected to client.
        """
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
         """
        Implements asynchat.async_chat's collect_incoming_data method.

        Appends data to internal read buffer.

        Parameters
        ----------
        data : str
            incoming data read by the async_chat channel.
        """
        self.logger.debug('collect_incoming_data')
        self.logger.debug('collect_incoming_data.  node_hash: {}'.format(self._server.node_hash))
        self.logger.debug('collect_incoming_data.  collected data: {}'.format(data))
        self._read_buffer.append(data)

    def found_terminator(self):
        """
        Implements asynchat.async_chat's found_terminator method.

        Handler for when message's terminator is found.  The message is converted into a json object, the read buffer flushed, and the message passed to a message handler.

        Parameters
        ----------
        data : str
            incoming data read by the async_chat channel.
        """
        request = json.loads(''.join(self._read_buffer))
        self.logger.debug('found_terminator.  request: {}'.format(request))
        self._read_buffer = []
        self._process_message(request)

    def _process_message(self, request):
        """
        Handles request messages passed from async_chat's found_terminator handler.

        Marks message as 'exteral request' and gives it a timestamp.  Hashes key before passing the request internally by instantiating an ExternalRequestCoordinator.
        """
        self.logger.debug('_request_handler.')

        request['type'] = 'external request'
        request['timestamp'] = util.current_time()

        try
            request['key'] = util.get_hash(request['key'])
        except:
            pass

        coordinator = ExternalRequestCoordinator(server=self._server, request=request)
        self._coordinators.append(coordinator)

        self.logger.debug('_request_handler. coordinator appended: {}'.format(coordinator))

    def _send_message(self, message):
        """
        Sends message back to client.
        """
        self.logger.debug('_send_message.')
        self.logger.debug('_send_message.  message {}'.format(message))
        self.push(util.pack_message(message, self._server._terminator))

        # set timeout to be 30 seconds after last request received
        self._timeout = util.add_time(util.current_time(), 30)

    def process(self):
        """
        Processes request queue and returns replies in the correct order when they are ready.
        """

        self.logger.debug('process')
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
    """
    Receives external requests and processes them internally.

    Instructs InternalRequestStage to handle request and listens for a reply, which it then forwards back to the client.
    """

    def __init__(self, server=None, request=None):
        """
        Parameters
        ----------
        server : PynamoServer object.
            object through which internal stages can be accessed.
        request : json
            request message.
        """
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._reply = None

        self._reply_listener = self._reply_listener()
        self._processor = self._request_handler(request)

    @property
    def completed(self):
        """
        Returns:
            True if request has returned a reply, False otherwise.
        """
        return bool(self._reply)

    @property
    def reply(self):
        return self._reply

    def process(self):
        """
        Steps processor through next cycle.
        """
        return self._processor.next()

    @util.coroutine
    def _request_handler(self, request):
        """
        Coroutine that acts as the ExternalRequestCoordinator's processor.

        Yields:
            True when request is complete.
            False when request is not yet complete.
        """
        self.logger.debug('_request_handler')
        self.logger.debug('_request_handler.  request: {}'.format(request))
        self._server.internal_request_stage.handle_internal_message(message=request, reply_listener=self._reply_listener)
        self.logger.debug('_request_handler. handle_internal_message called.')
        while True:
            yield self.completed

    @util.coroutine
    def _reply_listener(self):
        """
        Coroutine for listening to the reply of a request.
        """
        self.logger.debug('_reply_listener')
        self._reply = (yield)
        self.logger.debug('_reply_listener.  reply received: {}'.format(self._reply))
        yield True





