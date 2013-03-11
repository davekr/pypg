from psycopg2.extras import DictCursor

from manager import Manager

class DALCursor(DictCursor):
    """A cursor that keeps a list of column name -> index mappings.
        And logs every executed query."""

    def __init__(self, *args, **kwargs):
        super(DALCursor, self).__init__(*args, **kwargs)
        self._logger = Manager.get_logger()

    def log(self, msg):
        if msg: self._logger.debug(msg)

    def execute(self, query, vars=None):
        try:
            return super(DALCursor, self).execute(query, vars)
        finally:
            self.log(self.query)

    def callproc(self, procname, vars=None):
        try:
            return super(DALCursor, self).callproc(procname, vars)
        finally:
            self.log(self.query)

