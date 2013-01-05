from builder import SQLBuilder
from exception import DBException
from query import Query
from utils import TableValidator
from structure import Structure

class Row(TableValidator):

    def __init__(self, data, table):
        super(Row, self).__init__(table)
        self.data = data
        self._set_sql_builder()
        self._changed = False
        self._deleted = False
        
    def _set_sql_builder(self):
        self._sql = SQLBuilder(self._table_name)
        pks = Structure.get_primary_keys(self._table_name)
        map(lambda pk: self._sql.add_where_condition(pk, self.data[pk]), pks)
        
    def __str__(self):
        self._check_deleted()
        return str(self.data)
        
    def __getitem__(self, item):
        self._check_deleted()
        return self.data[item]
        
    def __setitem__(self, item, value):
        self._check_deleted()
        if self._check_column_in_table(item):
            self._sql.add_update_kwarg(item, value)
            self._changed = True
            self.data[item] = value
        
    def update(self, **kwargs):
        self._check_deleted()
        if self._changed or kwargs:
            map(self._check_column_in_table, kwargs.keys())
            self._sql.add_update_kwargs(kwargs)
            update_query, update_args = self._sql.build_update()
            Query().execute(update_query, update_args)
            self._changed = False
            #self._set_sql_builder()
            return self
        else:
            raise DBException('No data to update for this row.')
        
    def delete(self):
        self._check_deleted()
        Query().execute(*self._sql.build_delete())
        self._deleted = True
        return None
        
    def _check_deleted(self):
        if self._deleted:
            raise DBException('This row was deleted.')        
        
