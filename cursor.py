# -*- coding: utf-8 -*-

from psycopg2.extras import DictCursor
import time

from manager import Manager

class PyPgCursor(DictCursor):
    """Cursor, který obsahuje výsledky ve formě klíč-hodnota.
    Pokud je to nastaveno, loguje dotazy pomocí standardního logování a 
    pro každý dotaz měří čas provádění."""

    def __init__(self, *args, **kwargs):
        super(PyPgCursor, self).__init__(*args, **kwargs)
        self._logger = Manager.get_logger()

    def log(self, msg):
        if msg: self._logger.debug(msg)

    def execute(self, query, vars=None):
        self.timestamp = time.time()
        try:
            return super(PyPgCursor, self).execute(query, vars)
        finally:
            self.log(self.query)

    def callproc(self, procname, vars=None):
        self.timestamp = time.time()
        try:
            return super(PyPgCursor, self).callproc(procname, vars)
        finally:
            self.log(self.query)

