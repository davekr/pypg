import logging
import json
import time

from manager import Manager
from cursor import PyPgCursor
import settings

class Query(object):

    def __init__(self):
        self._conn = Manager.get_connection()
    
    def execute_and_fetch(self, sql, **kwargs):
        try:
            self._execute(sql, kwargs)
            data = self._cursor.fetchall()
        finally:
            self._done()
        return data
    
    def execute(self, sql, **kwargs):
        try:
            self._execute(sql, kwargs)
        finally:
            self._done()
        
    def _execute(self, sql, kwargs):
        self._cursor = self._conn.cursor(cursor_factory=PyPgCursor)
        self._cursor.execute(sql, kwargs.get('parameters'))
        if settings.LOG:
            self._log(kwargs)
        
    def _done(self):
        self._cursor.close()
        self._conn.commit()

    def _log(self, kwargs):
        t = (time.time() - self._cursor.timestamp) * 1000
        logger = logging.getLogger("PyPg.statistics")
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            logger.addHandler(logging.FileHandler("log/statistics.log"))
        logdict = {}
        logdict['query'] = self._cursor.query
        logdict['time'] = t
        logdict['datetime'] = time.strftime("%d.%m.%y %H:%M", time.localtime())
        logdict['tables'] = kwargs.get('tables')
        logdict['columns'] = map(str, kwargs.get('columns', []))
        logdict['rowcount'] = self._cursor.rowcount
        logger.debug(json.dumps(logdict))
        
        
