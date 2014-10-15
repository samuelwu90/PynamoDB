"""
    util.py
    ~~~~~~~~~~~~
    contains utility functions.
"""

import json
import hashlib
import datetime
import math


def current_time():
    return str(datetime.datetime.utcnow())

def add_time(timestamp, seconds):
    previous_timestamp = to_datetime(timestamp)
    return str(previous_timestamp + datetime.timedelta(seconds=seconds))

def to_datetime(timestamp):
    return datetime.datetime.strptime(str(timestamp), "%Y-%m-%d %H:%M:%S.%f")

def get_hash(value):
    """ Return a 32-byte hash of value as a hex string"""
    return hashlib.sha256(value).hexdigest()

def offset_hex(hex_string, offset=1):
    """ Returns hex string offset by the given amount.
        Useful for generating keys for which a given node_hash is reponsible, i.e. offset the node's hash by a negative amount
    """
    original_length = len(hex_string)
    offset_string = '{:x}'.format(int(hex_string, 16) + offset)
    padded_offset_string = (original_length - len(offset_string)) * '0' + offset_string
    return padded_offset_string

def pack_message(message, terminator):
    """ packs message for transport through asynchat channel"""
    return json.dumps(message) + terminator

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

def get_gossip_decay_factor(num_nodes, num_replicas):
    return 1 - pow(num_replicas, 2)/(num_nodes * math.log(num_nodes))

class ErrorCode(object):
    """ Object for passing around error codes from put/get/delete commands"""
    def __init__(self, error_code='\x00'):
        self.error_code = error_code

    def __eq__(self, other):
        return self.error_code == other.error_code

    def __nonzero__(self):
        if self.error_code == '\x00':
            return True
        else:
            return False

    def get_error_message(self):
        """returns a human-readable message for a given error."""
        try:
            return{
                '\x00': "Error code: {}.  Operation successful.".format([error_code]),
                '\x01': "Error code: {}.  Inexistant key.".format([error_code]),
                '\x02': "Error code: {}.  MemoryError.".format([error_code]),
                '\x03': "Error code: {}.  System overload.".format([error_code]),
                '\x04': "Error code: {}.  Internal KVStore failure.".format([error_code]),
                '\x05': "Error code: {}.  Unrecognized command.".format([error_code]),
                '\x06': "Error code: {}.  Unrecognized error: {}.".format([error_code], sys.exc_info()[0])
            }[self.error_code]
        except KeyError:
            self.logger.warn('Error code %s not found.', error, exc_info=True)
