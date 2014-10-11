import asynchat
import asyncore
import logging
import util
import json
import socket
import collections
import random

class InternalRequestStage(asyncore.dispatcher):

    """ Listens for external connections from other nodes and creates an InternalChannel upon accepting."""

    def __init__(self, server, hostname, internal_port):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.info('__init__')

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

        self.logger.debug('__init__ complete')

    def handle_accept(self):
        self.logger.info('handle_accept')
        if self._server.is_accepting_internal_requests:
            sock, client_address = self.accept()
            internal_channel = InternalChannel(server=self._server, sock=sock, client_address=client_address)
            self._channels.append(internal_channel)
            self.logger.debug('handle_accept.  accepting connection from: {}'.format(client_address))


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
            if coordinator.process():
                self._coordinators.remove(coordinator)

    def handle_internal_message(self, message=None, reply_listener=None, internal_channel=None):
        """ Send request to node_hash and report back to listener"""
        self.logger.debug('handle_internal_message.')
        if reply_listener:
            coordinator = InternalRequestCoordinator(server=self._server, message=message, reply_listener=reply_listener)

        elif internal_channel:
            coordinator = InternalRequestCoordinator(server=self._server, message=message, internal_channel=internal_channel)

        self._coordinators.append(coordinator)
        self.logger.debug('handle_internal_message. coordinator appended.')
    def _immediate_shutdown(self):
        self.handle_close()

class InternalChannel(asynchat.async_chat):
    """ Handles receiving requests from other nodes """

    def __init__(self, server=None, sock=None, client_address=None, node_hash=None, coordinator_listener=None):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.info('__init__')

        self._server = server
        self._read_buffer = []

        self.set_terminator(self._server.terminator)

        if sock:                # for incoming communication
            self._client_address = client_address
            asynchat.async_chat.__init__(self, sock)

        elif node_hash:     # for outgoing communication
            asynchat.async_chat.__init__(self)
            self._coordinator_listener = coordinator_listener
            hostname, port = self._server.membership_stage.node_address(node_hash)
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.connect((str(hostname), int(port)))
            except:
                self.logger.error('__init__.  failed to connect')

        self.logger.debug('__init__ complete')

    def collect_incoming_data(self, data):
        self.logger.info('collect_incoming_data')
        self._read_buffer.append(data)

    def found_terminator(self):
        self.logger.info('found_terminator')
        message = json.loads(''.join(self._read_buffer))
        self._read_buffer = []
        self._process_message(message=message, internal_channel=self)

    def _send_message(self, message):
        self.logger.info('_send_message')
        self.push(util.pack_message(message, self._server._terminator))
        self.logger.debug('_send_message.  message sent')

    def _send_gossip(self, message, propagation_probability=0.5):
        self.logger.info('_send_gossip')
        if random.random() > propagation_probability:
            self._send_message(message)

    def _process_message(self, message=None, internal_channel=None):
        self.logger.info('_process_message')
        self.logger.debug('_process_message.  node_hash: {}'.format(self._server.node_hash))

        if message['type'] == 'reply':
            self.logger.debug('_process_message.  type: {}, sending reply to coordinator listener'.format(message['type']))
            self._coordinator_listener.send(message)
        else:
            self.logger.debug('_process_message.  type: {} '.format(message['type']))
            self.logger.debug('_process_message.  calling handle_internal_message')
            self._server.internal_request_stage.handle_internal_message(message=message, internal_channel=internal_channel)



class InternalRequestCoordinator(object):

    def __init__(self, server=None, message=None, reply_listener=None, internal_channel=None, timeout=1000000, max_retries=3):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.info('__init__')
        self.logger.debug('__init__.  node_hash: {}'.format(server.node_hash))

        self._server = server
        self._message = message
        self._reply_listener = reply_listener
        self._internal_channel = internal_channel

        # for communications
        self._complete = False

        self._replies = dict()
        self._channels = list()

        # for timeouts
        self._timeout = util.add_time(util.current_time(), timeout)
        self._retries = dict()
        self._max_retries = max_retries
        self._outgoing_message = None

        #listener and processor
        self._coordinator_listener = self._coordinator_listener()
        self._processor = self._request_handler(message=message, reply_listener=reply_listener, internal_channel=internal_channel)

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
        return self._complete

    @util.coroutine
    def _coordinator_listener(self):
        self.logger.debug('_listener')

        for _ in xrange(self._server.num_replicas):
            reply = (yield)
            self._replies[reply['node_hash']] = reply
            self.logger.debug('_listener. reply received: {}'.format(reply))

        self._process_replies(self._message, self._replies)

        while True:
            yield True

    def _process_replies(self, request, replies):
        """ Process replies when listener has received
                If the request is get, return the most recent value and perform repairs if necessary.
        """
        self.logger.debug('_process_replies')

        if request['command'] in ['put', 'delete']:
            replies = [reply['error_code'] for reply in replies.values()]
            reply = {'error_code': max(set(replies), key=replies.count)}
            if self._reply_listener:
                self._reply_listener.send(reply)

        elif request['command'] == 'get':
            replies = sorted(self._replies.values(), key=lambda reply: reply['timestamp'])
            newest_reply = replies[0]
            reply = {
                'error_code': newest_reply['error_code'],
                'value': newest_reply['value']
            }
            if self._reply_listener:
                self._reply_listener.send(reply)
            # repair at read

        else:
            reply = replies[0]
            self.logger.debug('_process_replies.  reply: {}'.format(reply))

        self._complete = True

    @util.coroutine
    def _request_handler(self, message=None, reply_listener=None, internal_channel=None):
        self.logger.info('_request_handler')
        self.logger.debug('_request_handler')

        if message['type'] == 'external request':
            if message['command'] in ['put', 'get', 'delete']:
                self.logger.debug('_request_handler.  handling external {} command'.format(message['command']))

                responsible_node_hashes = self._server.membership_stage.get_responsible_node_hashes(message['key'])
                self.logger.debug('_request_handler.  key, node_hashes: {}, {}'.format(message['key'], responsible_node_hashes))

                for node_hash in responsible_node_hashes:
                    self._replies[node_hash] = None
                    self._retries[node_hash] = 0

                    if node_hash == self._server.node_hash:
                        reply = self._handle_request_locally(message)
                        self._coordinator_listener.send(reply)
                    else:
                        message['type'] = 'internal request'
                        self._handle_request_remotely(message, node_hash)
            elif message['command'] == 'shutdown':
                reply = {'error_code': '\x00'}
                reply_listener.send(reply)
                self._handle_shutdown()

        elif message['type'] == 'internal request':
            self.logger.debug('_request_handler.  handling internal {} command'.format(message['command']))
            reply = self._handle_request_locally(request=message)
            internal_channel._send_message(reply)
            internal_channel.close_when_done()

        elif message['type'] == 'reply':
            self._coordinator_listener.send(message)

        elif message['type'] == 'announced failure':
            reply = self._handle_announced_failure_message(message)
            internal_channel._send_message(reply)
            internal_channel.close_when_done()

        elif message['type'] == 'unannounced failure':
            reply = self._handle_unannounced_failure_message(message)
            internal_channel._send_message(reply)
            internal_channel.close_when_done()


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
            reply = self._server.persistence_stage.get(request['key'])
        elif request['command'] == 'delete':
            reply = self._server.persistence_stage.delete(request['key'])

        return reply
        self.logger.debug('_handle_request_locally.  returning reply: {}'.format(reply))

    def _handle_request_remotely(self, request, node_hash):
        self.logger.info('_handle_request_remotely')
        try:
            internal_channel = InternalChannel(server=self._server, node_hash=node_hash, coordinator_listener=self._coordinator_listener)
            self.logger.debug('_handle_request_remotely.  created internal channel.')
            internal_channel._send_message(request)
            self.logger.debug('_handle_request_remotely.  sending request.')
            self._channels.append(internal_channel)
            self.logger.debug('_handle_request_remotely.  appending internal channel.')
        except:
            pass

    def _handle_shutdown(self):
        # remove self from hash ring
        self._server.membership_stage.remove_hash(self._server.node_hash)
        #

        self._outgoing_message = {
            'type': 'announced failure',
            'node_hash': self._server.node_hash
        }

        node_hashes_to_be_notified = random.sample(self._serve.membership_stage.node_hashes, self._server.num_replicas)

        for node_hash in node_hashes_to_be_notified:
            self._replies[node_hash] = None
            self._retries[node_hash] = 0
            internal_channel = InternalChannel(server=self._server, node_hash=node_hash, coordinator_listener=self._coordinator_listener)
            internal_channel._send_message(self._outgoing_message)
            self._channels.append(internal_channel)

    def _handle_announced_failure(self):
        # attempt to remove node from own hash ring
        if self._server.membership_stage.remove_node_hash(self._server.node_hash):
            pass

    def _handle_timeout(self):
        """  Retries requests up to _max_num_tries, then considers
        """
        for node_hash, reply in self._replies:
            if not reply:
                if (self._retries[node_hash] < self._max_retries):
                    self._retries[node_hash] += 1
                    self._handle_request_remotely(self._request, node_hash)
                else:
                    self._server._handle_unannounced_failure(node_hash)
            else:
                self._coordinator_listener.send(reply)


