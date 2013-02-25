from row import Row
from column import Column
from structure import Structure
from query import Query
from builder import SQLBuilder
from cache import ResultSetCache

class ResultSet(object):

    def __init__(self, data, table_name, cache=None):
        self._table_name = table_name
        self._data = data
        self._cache = cache if cache else ResultSetCache(table_name, data)
        
    def __str__(self):
        return self.pretty_print()
    
    def __getitem__(self, item):
        return Row(self._data[item], self._table_name)
        
    def __len__(self):
        return len(self._data)
        
    def pretty_print(self):
        return '\n'.join([str(row) for row in self._data])
        
    def __iter__(self):
        return iter(self._get_referenced_data())

    def _get_referenced_data(self):
        return map(lambda row: Row(row, self._table_name, self), self._data)

    def _get_fk_data(self, table_name, fk, fk_value):
        reltable = Structure.get_fk_referenced_table(table_name, fk)
        reltable_pk = Structure.get_primary_key(reltable)
        if not self._cache.relation_exists(table_name, reltable):
            sql = SQLBuilder(reltable)
            sql.add_where_literal(Column(reltable_pk).in_(self._cache.get_all_keys(table_name, fk)))
            data = Query().execute_and_fetch(*sql.build_select())
            self._cache.save_relation(table_name, reltable, data)
        return Row(self._cache.get_relation_row(reltable, reltable_pk, fk_value), reltable, self)

    def _get_rel_data(self, relation, pk, pk_value):
        relation_fk = Structure.get_foreign_key_for_table(relation, self._table_name)
        if not self._cache.relation_exists(self._table_name, relation):
            sql = SQLBuilder(relation)
            sql.add_where_literal(Column(relation_fk).in_(self._cache.get_all_keys(self._table_name, pk)))
            data = Query().execute_and_fetch(*sql.build_select())
            self._cache.save_relation(self._table_name, relation, data)
        return ResultSet(self._cache.get_relation_set(relation, relation_fk, pk_value), relation, self._cache)

