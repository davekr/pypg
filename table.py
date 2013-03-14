from builder import SQLBuilder
from resultset import ResultSet, ReadOnlyResultSet
from column import Column
from query import Query
from structure import Structure
from utils import TableValidator
from exception import DBException
    
__all__ = ['Table']

class TableSelect(TableValidator):

    def __init__(self, name, sql):
        super(TableSelect, self).__init__(name)
        self._sql = sql
        self._data = None
    
    def __str__(self):
        return 'Table: %s' % self._table_name
        
    def limit(self, limit):
        limit = self._check_limit(limit)
        self._sql.add_limit_condition(limit)
        return self._table_select_instance()
    
    def order(self, order):
        self._check_is_instance(order, 'Column')
        self._sql.add_order_condition(order)
        return self._table_select_instance()

    def order_desc(self, order):
        self._check_is_instance(order, 'Column')
        self._sql.add_order_desc_condition(order)
        return self._table_select_instance()
    
    def where(self, *args, **kwargs):
        self._validate_where(args, kwargs)
        self._sql.add_where_conditions(kwargs)
        self._sql.add_where_literals(args)
        return self._table_where_instance()

    def select(self, *args):
        if args:
            map(lambda arg: self._check_is_instance(arg, 'Column'), args)
            args = list(args)
            args.append(Column(self._table_name, Structure.get_primary_key(self._table_name)))
            self._sql.add_select_args(args)
        return self._table_selected_instance()

    def join(self, table, on=None):
        self._check_is_instance(table, 'Table')
        if on:
            self._check_is_instance(on, 'Literal')
            self._validate_on(table, on)
        else:
            Structure.tables_related(self._table_name, table._table_name)
            try:
                fk = Structure.get_foreign_key_for_table(self._table_name, table._table_name)
            except DBException:
                fk = Structure.get_foreign_key_for_table(table._table_name, self._table_name)
                on = Column(table._table_name, fk) == \
                        Column(self._table_name, Structure.get_primary_key(self._table_name))
            else:
                on = Column(self._table_name, fk) == \
                        Column(table._table_name, Structure.get_primary_key(table._table_name))
        self._sql.add_join(table._table_name, on)
        return self._table_select_instance()
            
    def _table_select_instance(self):
        return TableSelect(self._table_name, self._sql)
        
    def _table_selected_instance(self):
        return TableSelected(self._table_name, self._sql)

    def _table_where_instance(self):
        return TableWhere(self._table_name, self._sql)

class TableSelected(TableSelect):

    def __getitem__(self, item):
        data = self._get_data()
        return data[item]
        
    def __len__(self):
        data = self._get_data()
        return len(data)
        
    def __iter__(self):
        data = self._get_data()
        return iter(data)

    def pretty_print(self):
        data = self._get_data()
        return '\n'.join([str(row) for row in data])

    def _get_data(self):
        if not self._data:
            data = Query().execute_and_fetch(**self._sql.build_select())
            if len(self._sql._tables) > 1:
                cls = ReadOnlyResultSet
            else:
                cls = ResultSet
            self._data = cls(data, self._table_name)
        return self._data

    def _table_select_instance(self):
        return self
        
    def _table_selected_instance(self):
        return self

    def _table_where_instance(self):
        return self

class TableWhere(TableSelect):
    
    def delete(self):
        Query().execute(**self._sql.build_delete())
        return None
        
    def update(self, **kwargs):
        self._validate_update(kwargs)
        self._sql.add_update_kwargs(kwargs)
        Query().execute(**self._sql.build_update())
        return None

    def update_and_get(self, **kwargs):
        self._validate_update(kwargs)
        self._sql.add_update_kwargs(kwargs)
        self._sql.add_returning_all()
        data = Query().execute_and_fetch(**self._sql.build_update())
        return ResultSet(data, self._table_name)
        
class Table(TableWhere):

    def __init__(self, name):
        super(Table, self).__init__(name, SQLBuilder(name))

    def insert(self, *args, **kwargs):
        self._validate_insert(args, kwargs)
        self._sql.add_insert_kwargs(kwargs)
        Query().execute(**self._sql.build_insert())
        return None
     
    def insert_and_get(self, *args, **kwargs):
        self._validate_insert(args, kwargs)
        self._sql.add_insert_kwargs(kwargs)
        self._sql.add_returning_all()
        data = Query().execute_and_fetch(**self._sql.build_insert())
        return ResultSet(data, self._table_name)

    def row(**kwargs):
        self._validate_row(kwargs)
        
    def __getattr__(self, attr):
        if Structure.table_has_column(self._table_name, attr):
            return Column(self._table_name, attr)
