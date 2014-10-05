from util import ErrorCode
from persistence_engine import PersistenceEngine

class PersistenceStage(object):
    """ Stage for managing key-value persistence."""

    def __init__(self):
        self._persistence_engine = PersistenceEngine()

    def keys(self):
        return self._persistence_engine.keys()

    def put(self, key, value):
        """ Returns:
                error code \x00 if put is successful
                error code \x06 if unknown error is encountered

            Compares timestamps of old and new values if key is already present in hash ring
        """

        try:
            old_timestamp = self._persistence_engine.get(key).timestamp
        except KeyError:
            self._persistence_engine.put(key, value)
        except:
            return ErrorCode('\x06')
        else:
            if value.timestamp > old_timestamp:
                self._persistence_engine.put(key, value)

        return ErrorCode('\x00')

    def get(self, key):
        """ Returns:
                (error code \x00, value) if get is successful
                (error code \x01, None) if key inexistant
                (error code \x06, None) if unknown error is encountered
        """

        try:
            value = self._persistence_engine.get(key)
        except KeyError:
            return (ErrorCode('\x01'), None)
        except:
            return (ErrorCode('\x06'), None)
        else:
            return (ErrorCode('\x00'), value)

    def delete(self, key):
        """ Returns:
                error code \x00 if delete is successful
                error code \x01 if key inexistant
                error code \x06 if unknown error is encountered
        """

        try:
            self._persistence_engine.delete(key)
        except KeyError:
            return ErrorCode('\x01')
        except:
            return ErrorCode('\x06')
        else:
            return ErrorCode('\x00')
