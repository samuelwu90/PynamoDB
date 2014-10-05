"""
    persistence_engine.py
    ~~~~~~~~~~~~
    Implements put, get, delete methods for PersistenceStage.  Using an actual persistence engine (i.e. MySQL, BDB), one would implement the three methods themselves.
    """


class PersistenceEngine(object):
    """ Basic persistence engine implemented as a regular Python dict."""

    def __init__(self):
        self._persistence = dict()

    def keys():
        return self._persistence.keys()

    def put(self, key, value):
        """ Put key value pair into storage"""
        self._persistence[key] = value

    def get(self, key):
        """ Get key's value """
        return self._persistence[key]

    def delete(self, key):
        """ Delete key value pair """
        del self._persistence[key]
