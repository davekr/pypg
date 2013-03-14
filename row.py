from builder import SQLBuilder
from exception import DBException
from query import Query
from utils import TableValidator
from structure import Structure

class Row(TableValidator):

    def __init__(self, data, table, result_set=None):
        super(Row, self).__init__(table)
        self.data = data
        self._set_sql_builder()
        self._changed = False
        self._deleted = False
        self._result_set = result_set
        
    def _set_sql_builder(self):
        self._sql = SQLBuilder(self._table_name)
        pk = self._get_pk()
        self._sql.add_where_condition(pk, self.data[pk])

        
    def _get_pk(self):
        pk= Structure.get_primary_key(self._table_name)
        if self.data.get(pk) is not None:
            return pk
        else:
            raise DBException('Incorectly formated settings.PK_NAMING')

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

    def __getattr__(self, attr):
        self._check_deleted()
        if Structure.is_foreign_key(self._table_name, attr):
            if self._result_set:
                return self._result_set._get_fk_data(self._table_name, attr, self.data[attr])
            else:
                reltable = Structure.get_fk_referenced_table(self._table_name, attr)
                reltable_pk = Structure.get_primary_key(reltable)
                sql = SQLBuilder(reltable)
                sql.add_where_condition(reltable_pk, self.data[attr])
                data = Query().execute_and_fetch(**sql.build_select())
                return Row(data[0], reltable)
        else:
            self._check_relation_exists(attr)
            pk = self._get_pk()
            if self._result_set:
                return self._result_set._get_rel_data(attr, pk, self.data[pk])
            else:
                relation_fk = Structure.get_foreign_key_for_table(attr, self._table_name)
                sql = SQLBuilder(attr)
                sql.add_where_condition(relation_fk, self.data[pk])
                data = Query().execute_and_fetch(**sql.build_select())
                from resultset import ResultSet
                return ResultSet(data, attr)

        
    def update(self, **kwargs):
        self._check_deleted()
        if self._changed or kwargs:
            map(self._check_column_in_table, kwargs.keys())
            self._sql.add_update_kwargs(kwargs)
            Query().execute(**self._sql.build_update())
            self._changed = False
            #self._set_sql_builder()
            return self
        else:
            raise DBException('No data to update for this row.')
        
    def delete(self):
        self._check_deleted()
        Query().execute(**self._sql.build_delete())
        self._deleted = True
        return None
        
    def _check_deleted(self):
        if self._deleted:
            raise DBException('This row was deleted.')  
            
    def _check_relation_exists(self, relation):
        Structure.table_exists(relation) 
        Structure.tables_related(relation, self._table_name)
        
    def add_reference(self, result_set):
        self._result_set = result_set

