"""
    util.py
    ~~~~~~~~~~~~
    contains utility functions.
"""

import hashlib
from datetime import datetime


# Classes

def get_timestamp():
    return datetime.utcnow()

class Key(object):
    """ A simple object for adding aditional properties to keys """

    def __init__(self, key=None, node_hash=None):
        self.key = key
        self.node_hash  = node_hash

class Value(object):
    """ A simple object for adding aditional properties to values """

    def __init__(self, value=None, timestamp=None):
        self.value = value
        if not timestamp:
            self.timestamp = get_timestamp()
        else:
            self.timestamp = timestamp

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
