from row import Row, ReadOnlyRow
from column import Column
from structure import Structure
from query import Query
from builder import SQLBuilder
from cache import ResultSetCache

class ReadOnlyResultSet(object):

    def __init__(self, data, table_name):
        self._table_name = table_name
        self._data = data
        
    def __str__(self):
        return self.pretty_print()
    
    def __getitem__(self, item):
        return ReadOnlyRow(self._data[item], self._table_name)
        
    def __len__(self):
        return len(self._data)
        
    def pretty_print(self):
        return '\n'.join([str(row) for row in self._data])
        
    def __iter__(self):
        return iter([ReadOnlyRow(row, self._table_name) for row in self._data])

class ResultSet(ReadOnlyResultSet):

    def __init__(self, data, table_name, cache=None):
        super(ResultSet, self).__init__(data, table_name)
        self._cache = cache if cache else ResultSetCache(table_name, data)
    
    def __getitem__(self, item):
        return Row(self._data[item], self._table_name)
        
    def __iter__(self):
        return iter(self._get_referenced_data())

    def _get_referenced_data(self):
        return map(lambda row: Row(row, self._table_name, self), self._data)

    def _get_fk_data(self, table_name, fk, fk_value):
        reltable = Structure.get_fk_referenced_table(table_name, fk)
        reltable_pk = Structure.get_primary_key(reltable)
        if not self._cache.relation_exists(table_name, reltable):
            sql = SQLBuilder(reltable)
            sql.add_where_literal(Column(reltable, reltable_pk).in_(self._cache.get_all_keys(table_name, fk)))
            data = Query().execute_and_fetch(**sql.build_select())
            self._cache.save_relation(table_name, reltable, data)
        return Row(self._cache.get_relation_row(reltable, reltable_pk, fk_value), reltable, self)

    def _get_rel_data(self, table_name, relation, pk, pk_value):
        self._restricted_table_call = {'table_name': table_name, 'relation': relation, 'pk': pk, 'pk_value': pk_value}
        from restricted import RestrictedTableSelect
        return RestrictedTableSelect(relation, SQLBuilder(relation), self)

    def _get_rel_data_restricted(self, sql):
        saved = self._restricted_table_call
        table_name, relation, pk, pk_value = saved['table_name'], saved['relation'], saved['pk'], saved['pk_value']
        relation_fk = Structure.get_foreign_key_for_table(relation, table_name)
        if not self._cache.relation_exists(table_name, relation):
            if sql._select_args:
                sql.add_select_arg(Column(relation, relation_fk))
            if sql._limit:
                union_sql = []
                union_parameters = []
                union_dict = {}
                import copy
                for id in self._cache.get_all_keys(table_name, pk):
                    limiting_sql = copy.deepcopy(sql)
                    limiting_sql.add_where_literal(Column(relation, relation_fk) == id)
                    union_dict = limiting_sql.build_select() 
                    union_sql.append('(%s)' % union_dict['sql'])
                    union_parameters.extend(union_dict['parameters'])
                union_dict['sql'], union_dict['parameters'] = ' UNION '.join(union_sql), union_parameters
                data = Query().execute_and_fetch(**union_dict)
                self._cache.save_relation(table_name, relation, data)
            else:
                sql.add_where_literal(Column(relation, relation_fk).in_(self._cache.get_all_keys(table_name, pk)))
                data = Query().execute_and_fetch(**sql.build_select())
                self._cache.save_relation(table_name, relation, data)
        return ResultSet(self._cache.get_relation_set(relation, relation_fk, pk_value), relation, self._cache)

