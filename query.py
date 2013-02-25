from psycopg2 import extras
from manager import Manager
import settings

class Query(object):

    def __init__(self):
        self._conn = Manager.get_connection()
    
    def execute_and_fetch(self, sql, *args):
        self._execute(sql, args)
        data = self._cursor.fetchall()
        self._done()
        return data
    
    def execute(self, sql, *args):
        self._execute(sql, args)
        self._done()
        
    def _execute(self, sql, args):
        self._cursor = self._conn.cursor(cursor_factory=extras.DictCursor)
        if settings.DEBUG:
            print self._cursor.mogrify(sql, *args)
        self._cursor.execute(sql, *args)
        
    def _done(self):
        self._cursor.close()
        self._conn.commit()
        
        
