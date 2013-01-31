from table import TableSelect
from resultset import ResultSet

class RestrictedTableSelect(TableSelect):

    def __init__(self, name, sql, cache, parrent, rel_fk, pk_val):
        super(RestrictedTableSelect, self).__init__(name, sql)
        self._cache, self._parrent = cache, parrent
        self._rel_fk, self._pk_val = rel_fk, pk_val

    def limit(self, limit):
        super(RestrictedTableSelect, self).limit(limit)
        return self

    def order(self, order):
        super(RestrictedTableSelect, self).order(order)
        return self

    def where(self, *args, **kwargs):
        super(RestrictedTableSelect, self).where(*args, **kwargs)
        return self
    
    def _parse_data(self, data):
        self._cache.save_relation(self._parrent, self._table_name, data)
        return ResultSet(self._cache.get_relation_set(self._table_name, self._rel_fk, self._pk_val), self._table_name, self._cache)

class RestrictedDummyTable(TableSelect):

    def __init__(self, name, data, cache):
        super(RestrictedDummyTable, self).__init__(name, None)
        self._data = data
        self._cache = cache

    def limit(self, limit):
        return self

    def order(self, order):
        return self

    def where(self, *args, **kwargs):
        return self

    def select(self, *args):
        return ResultSet(self._data, self._table_name, self._cache)
    
