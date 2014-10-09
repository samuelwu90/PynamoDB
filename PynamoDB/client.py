import asyncore
import asynchat
import socket
import json
import util
import logging

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
            self.connect((host, port))
        except:
            print "Refused"

    @property
    def requests(self):
        return self._requests

    @property
    def replies(self):
        return self._replies

    def _immediate_shutdown(self):
        self.logger.info('_immediate_shutdown')
        self.handle_when_done()

    def send_message(self, message):
        self.logger.info('send_message')
        self.logger.debug('send_message.  message: {}'.format(message))
        self.push(util.pack_message(message, self.terminator))

    def put(self, key, value):
        self.logger.info('put')
        message = {
            'command' : 'put',
            'key' : key,
            'value' : value
        }
        self._requests.append(message)
        self.send_message(message)

    def get(self, key, value):
        self.logger.info('get')
        message = dict()
        message = {
            'command' : 'put',
            'key' : key
        }
        self._requests.append(message)
        self.send_message(message)

    def delete(self, key, value):
        self.logger.info('delete')
        message = dict()
        message = {
            'command' : 'delete',
            'key' : key
        }
        self._requests.append(message)
        self.send_message(message)

    def shutdown(self, key, value):
        self.logger.info('shutdown')
        message = dict()
        message = {
            'command' : 'shutdown'
        }
        self._requests.append(message)
        self.send_message(message)

    def collect_incoming_data(self, data):
        self.logger.info('collect_incoming_data')
        self.logger.debug('collect_incoming_data.  data: {}'.format(data))
        self._read_buffer.append(data)

    def found_terminator(self):
        self.logger.info('found_terminator')
        reply = json.loads(''.join(self._read_buffer))
        self._read_buffer = []
        self._handle_reply(reply)

    def _handle_reply(self, reply):
        self.logger.debug('_handle_reply')
        self.logger.debug('_handle_reply.  appended reply: {}'.format(reply))
        self._replies.append(reply)

