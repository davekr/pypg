
class ResultSet(object):

    def __init__(self, conn, sql, columns=[]):
        self._conn = conn
        self._sql = sql
        self._data = []
        self._columns = columns
        
    def __str__(self):
        return self._sql   
    
    def __getitem__(self, item):
        data = self._get_data()
        return dict(zip(self._columns, data[item]))
        
    def print_all(self):
        data = self._get_data()
        print '\n'.join([str(dict(zip(self._columns, row))) for row in data])
        
    def _get_data(self):
        if not self._data:
            cursor = self._conn.cursor()
            cursor.execute(*self._sql)
            self._data = cursor.fetchall()
            cursor.close()
        return self._data
