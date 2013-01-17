from builder import SQLBuilder
from resultset import ResultSet
from column import Column
from query import Query
from row import Row
from structure import Structure
from utils import TableValidator
    
__all__ = ['Table']
    
class TableSelect(TableValidator):

    def __init__(self, name, sql):
        super(TableSelect, self).__init__(name)
        self._sql = sql
    
    def __str__(self):
        return 'Table: %s' % self._table_name
        
    def limit(self, limit):
        limit = self._check_limit(limit)
        self._sql.add_limit_condition(limit)
        return self._table_select_instance()
    
    def order(self, order):
        self._check_column_in_table(order)
        self._sql.add_order_condition(order)
        return self._table_select_instance()
    
    def where(self, *args, **kwargs):
        self._validate_where(args, kwargs)
        self._sql.add_where_conditions(kwargs)
        self._sql.add_where_literals(args)
        return self._table_where_instance()
    
    def select(self, *args):
        if args:
            map(self._check_column_in_table, args)
            args = self._add_primary_keys(list(args))
            self._sql.add_select_args(args)
        select_query, select_args = self._sql.build_select()
        data = Query().execute_and_fetch(select_query, select_args)
        data = self._parse_data(data, args)
        return data
            
    def _add_primary_keys(self, args):
        for pk in Structure.get_primary_keys(self._table_name):
            if pk not in args:
                args.append(pk)
        return args
            
    def _table_select_instance(self):
        return TableSelect(self._table_name, self._sql)
        
    def _table_where_instance(self):
        return TableWhere(self._table_name, self._sql)
        
    def _parse_data(self, data, args=None):
        return ResultSet([Row(row, self._table_name) for row in data])
    
class TableWhere(TableSelect):
    
    def delete(self):
        Query().execute(*self._sql.build_delete())
        return None
        
    def update(self, **kwargs):
        self._validate_update(kwargs)
        self._sql.add_update_kwargs(kwargs)
        update_query, update_args = self._sql.build_update()
        Query().execute(update_query, update_args)
        return None

    def update_and_get(self, **kwargs):
        self._validate_update(kwargs)
        self._sql.add_update_kwargs(kwargs)
        self._sql.add_returning_args(Structure.get_all_columns(self._table_name))
        update_query, update_args = self._sql.build_update()
        data = Query().execute_and_fetch(update_query, update_args)
        data = self._parse_data(data)
        return data
        
class Table(TableWhere):

    def __init__(self, name):
        super(Table, self).__init__(name, SQLBuilder(name))

    def insert(self, *args, **kwargs):
        self._validate_insert(args, kwargs)
        self._sql.add_insert_kwargs(kwargs)
        insert_query, insert_args = self._sql.build_insert()
        Query().execute(insert_query, insert_args)
        return None
     
    def insert_and_get(self, *args, **kwargs):
        self._validate_insert(args, kwargs)
        self._sql.add_insert_kwargs(kwargs)
        self._sql.add_returning_args(Structure.get_all_columns(self._table_name))
        insert_query, insert_args = self._sql.build_insert()
        data = Query().execute_and_fetch(insert_query, insert_args)
        data = self._parse_data(data)
        return data
        
    def row(**kwargs):
        self._validate_row(kwargs)
        
    def __getattr__(self, attr):
        if Structure.table_has_column(self._table_name, attr):
            return Column(attr)
        else:
            raise AttributeError("'%s' object has no attribute '%s'" % (type(self), attr))
            
    

