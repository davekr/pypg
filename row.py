# -*- coding: utf-8 -*- 

from builder import SQLBuilder
from exception import PyPgException
from query import Query
from utils import TableValidator
from structure import Structure

class ReadOnlyRow(TableValidator):
    """Reprezentace řádku z výsledku SQL dotazu, který používal spojení tabulek 
    nebo neobsahuje primární klíče. Umožňuje data řádku pouze číst."""

    def __init__(self, data, table):
        super(ReadOnlyRow, self).__init__(table)
        self.data = data
        self._deleted = False

    def __str__(self):
        self._check_deleted()
        return str(self.data)
        
    def __getitem__(self, item):
        self._check_deleted()
        return self.data[item]

    def keys(self):
        self._check_deleted()
        return self.data.keys()
    
    def values(self):
        self._check_deleted()
        return self.data.values()

    def items(self):
        self._check_deleted()
        return self.data.items()

    def _check_deleted(self):
        if self._deleted:
            raise PyPgException('This row was deleted.')  

class Row(ReadOnlyRow):
    """Třída představuje řádek výsledku vráceného na SQL dotaz. Umožňuje s řádkem 
    dále pracovat, poskytuje metody pro aktualizaci a mazání a metody pro přístup k vazebním objektům řádků."""

    def __init__(self, data, table, result_set=None):
        super(Row, self).__init__(data, table)
        self._set_sql_builder()
        self._changed = False
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
            raise PyPgException('Incorectly formated naming for primary key. Please provide manager.Naming instance or set strict mode on.')
        
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
                return self._result_set._get_rel_data(self._table_name, attr, pk, self.data[pk])
            else:
                self._restricted_table_attr = attr
                from restricted import RestrictedTableSelect
                return RestrictedTableSelect(attr, SQLBuilder(attr), self)

    def _get_rel_data_restricted(self, sql):
        attr, pk = self._restricted_table_attr, self._get_pk()
        relation_fk = Structure.get_foreign_key_for_table(attr, self._table_name)
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
            self._set_sql_builder()
            return self
        else:
            raise PyPgException('No data to update for this row.')
        
    def delete(self):
        self._check_deleted()
        Query().execute(**self._sql.build_delete())
        self._deleted = True
        return None
            
    def _check_relation_exists(self, relation):
        Structure.table_exists(relation) 
        Structure.tables_related(relation, self._table_name)

