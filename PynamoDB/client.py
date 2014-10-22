import asyncore
import asynchat
import socket
import json
import util
import logging
import sys

class PynamoClient(asynchat.async_chat):

    def __init__(self, host, port):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        asynchat.async_chat.__init__(self)
        self.host = host
        self.port = port
        self._read_buffer = []
        self._write_buffer = []

        self._requests = []
        self._replies = []

        self.terminator = "\r\n"
        self.set_terminator(self.terminator)

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.connect((host, int(port)))
        except:
            self.logger.error('__init__.  connection refused.')

    @property
    def requests(self):
        return self._requests

    @property
    def replies(self):
        return self._replies

    def _immediate_shutdown(self):
        self.logger.debug('_immediate_shutdown')
        self.close_when_done()

    def handle_error(self):
        self.logger.error( sys.exc_info() )

    def send_message(self, message):
        self.logger.debug('send_message')
        self.logger.debug('send_message.  message: {}'.format(message))
        self.push(util.pack_message(message, self.terminator))

    def put(self, key, value):
        self.logger.debug('put')
        message = {
            'command' : 'put',
            'key' : key,
            'value' : value
        }
        self._requests.append(message)
        self.send_message(message)

    def get(self, key):
        self.logger.debug('get')
        message = {
            'command' : 'get',
            'key' : key
        }
        self._requests.append(message)
        self.send_message(message)

    def delete(self, key):
        self.logger.debug('delete')
        message = {
            'command' : 'delete',
            'key' : key
        }
        self._requests.append(message)
        self.send_message(message)

    def shutdown(self):
        self.logger.debug('shutdown')
        message = {
            'command' : 'shutdown'
        }
        self._requests.append(message)
        self.send_message(message)

    def collect_incoming_data(self, data):
        self.logger.debug('collect_incoming_data')
        self.logger.debug('collect_incoming_data.  data: {}'.format(data))
        self._read_buffer.append(data)

    def found_terminator(self):
        self.logger.debug('found_terminator')
        reply = json.loads(''.join(self._read_buffer))
        self._read_buffer = []
        self._handle_reply(reply)

    def _handle_reply(self, reply):
        self.logger.debug('_handle_reply')
        self.logger.debug('_handle_reply.  appended reply: {}'.format(reply))
        self._replies.append(reply)

