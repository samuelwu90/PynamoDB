import logging
import util
from persistence_engine import PersistenceEngine

class PersistenceStage(object):
    """ Stage for managing key-value persistence."""

    def __init__(self, server=None):
        self.logger = logging.getLogger('{}'.format(self.__class__.__name__))
        self.logger.debug('__init__')

        self._server = server
        self._persistence_engine = PersistenceEngine()

    def keys(self):
        return self._persistence_engine.keys()

    def partition_keys(self):
        """ Returns:
                a dict() where:
                    keys: node_hashes
                    values: keys for which the given node_hash is responsible
        """
        partitioned_keys = collections.defaultdict(list)

        for key in self.server.persistence_stage.keys():
            partitioned_keys[key.node_hash].append(key)

        return partitioned_keys

    def put(self, key, value, timestamp=None):
        """ Returns:
                error code \x00 if put is successful
                error code \x06 if unknown error is encountered

            Compares timestamps of old and new values if key is already present in hash ring
        """

        self.logger.debug('put')

        reply = {
                        'node_hash' : self._server.node_hash,
                        'error_code' : None
                        }
        try:
            if not timestamp:
                new_timestamp = util.get_timestamp()
            else:
                new_timestamp = timestamp
            try:
                old_timestamp = self._persistence_engine.get(key)['timestamp']
            except KeyError:
                self._persistence_engine.put(key, value, new_timestamp)
                reply['error_code'] = '\x00'
            except:
                reply['error_code'] = '\x06'
            else:
                if new_timestamp > old_timestamp:
                    self._persistence_engine.put(key, value, new_timestamp)
                    reply['error_code'] = '\x00'
            finally:
                return reply
        except Exception as e:
            print e, sys.exc_info()

    def get(self, key):
        """ Returns:
                (error code \x00, value) if get is successful
                (error code \x01, None) if key inexistant
                (error code \x06, None) if unknown error is encountered
        """

        self.logger.debug('get')

        reply = {
                        'node_hash' : self._server.node_hash,
                        'error_code' :  None,
                        'value' : None,
                        'timestamp': None
                        }

        try:
            value, timestamp = self._persistence_engine.get(key)
        except KeyError:
            reply['error_code'] = '\x01'
        except:
            reply['error_code'] = '\x06'
        else:
            reply['error_code'] = '\x00'
        finally:
            return reply

    def delete(self, key):
        """ Returns:
                error code \x00 if delete is successful
                error code \x01 if key inexistant
                error code \x06 if unknown error is encountered
        """

        self.logger.debug('delete')

        reply = {
                        'node_hash' : self._server.node_hash,
                        'error_code' : None
                        }
        try:
            self._persistence_engine.delete(key)
        except KeyError:
            reply['error_code'] = '\x01'
        except:
            reply['error_code'] = '\x06'
        else:
            reply['error_code'] = '\x00'
        finally:
            return reply

