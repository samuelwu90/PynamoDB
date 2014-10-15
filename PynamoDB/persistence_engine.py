"""
    persistence_engine.py
    ~~~~~~~~~~~~
    Implements put, get, delete methods for PersistenceStage.  Using an actual persistence engine (i.e. MySQL, BDB), one would implement the three methods themselves.
    """


class PersistenceEngine(object):
    """ Basic persistence engine implemented as a regular Python dict."""

    def __init__(self):
        self._persistence = dict()

    def keys(self):
        return self._persistence.keys()

    def put(self, key, value, timestamp):
        """ Put key value pair into storage"""
        self._persistence[key] = {'value': value, 'timestamp': timestamp}
        return True

    def get(self, key):
        """ Get key's value """
        return self._persistence[key]['value'], self._persistence[key]['timestamp']

    def delete(self, key):
        """ Delete key value pair """
        del self._persistence[key]
        return True
