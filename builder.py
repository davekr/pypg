
class SQLBuilder(object):
    
    SELECT = 'SELECT %(args)s FROM %(table)s %(join)s %(where)s %(order)s %(limit)s'
    INSERT = 'INSERT INTO %(table)s (%(args)s) VALUES (%(values)s) %(returning)s'
    DELETE = 'DELETE FROM %(table)s %(where)s'
    UPDATE = 'UPDATE %(table)s SET %(values)s %(where)s %(returning)s'
    
    def __init__(self, table):
        self._table = self._escape(table)
        self._select_args = []
        self._where = ''
        self._where_values = []
        self._join = ''
        self._tables = [table]
        self._order = ''
        self._limit = ''
        self._limit_to = 0
        self._insert_keys = []
        self._insert_values = []
        self._update = ''
        self._update_values = []
        self._returning = ''
        
    def add_select_args(self, args):
        map(self.add_select_arg, args)
        
    def add_select_arg(self, arg):
        self._select_args.append(arg)

    def add_aliases_to_select_args(self):
        self._select_args = ['%s AS %s' % (arg, arg.get_aliased_name()) for arg in self._select_args]
            
    def add_insert_kwargs(self, kwargs):
        map(lambda item: self.add_insert_kwarg(*item), kwargs.items())
            
    def add_insert_kwarg(self, key, arg):
        self._insert_keys.append(self._escape(key))
        self._insert_values.append(arg)
        
    def add_update_kwargs(self, kwargs):
        map(lambda item: self.add_update_kwarg(*item), kwargs.items())
        
    def add_update_kwarg(self, key, value):
        self._update_values.append(value)
        if not self._update:
            self._update = '"%s"=%%s' % key
        else:
            self._update += ', "%s"=%%s' % key
            
    def add_where_conditions(self, kwargs):
        map(lambda item: self.add_where_condition(*item), kwargs.items())
        
    def add_where_condition(self, key, value):
        self._where_values.append(value)
        if not self._where:
            self._where = 'WHERE "%s"=%%s ' % key
        else:
            self._where += 'AND "%s"=%%s ' % key
    
    def add_where_literals(self, args):
        map(self.add_where_literal, args)
            
    def add_where_literal(self, arg):
        string, value = arg.get()
        if type(value) == list or type(value) == tuple:
            self._where_values.extend(value)
        else:
            self._where_values.append(value)
        if not self._where:
            self._where = 'WHERE %s' % string
        else:
            self._where += ' AND %s' % string
            
    def add_returning_args(self, args):
        map(self.add_returning_arg, args)
        
    def add_returning_arg(self, arg):
        if not self._returning:
            self._returning = 'RETURNING "%s"' % arg
        else:
            self._returning += ', "%s"' % arg

    def add_returning_all(self):
        self._returning = 'RETURNING *'
            
    def add_order_condition(self, order_by):
        self._order = 'ORDER BY %s' % order_by
        
    def add_order_desc_condition(self, order_by):
        self._order = 'ORDER BY %s DESC' % order_by

    def add_limit_condition(self, limit_to):
        self._limit = 'LIMIT %s'
        self._limit_to = limit_to
        
    def add_join(self, table, condition):
        self._join += 'JOIN "%s" ON %s ' % (table, condition)
        self._tables.append(table)
        
    def build_select(self):
        select = self.SELECT % ({'table': self._table, 'args': ', '.join(self._select_args) if self._select_args else '*', \
                                 'where': self._where, 'order': self._order, 'limit': self._limit, 'join': self._join})
        parameters_list = self._where_values[:]
        if self._limit_to:
            parameters_list.append(self._limit_to)
        return {'sql': select, 'parameters': parameters_list, 'tables': self._tables, 'columns': self._select_args}
        
    def build_delete(self):
        delete = self.DELETE % ({'table': self._table, 'where': self._where})
        return {'sql': delete, 'parameters': self._where_values, 'tables': [], 'columns': []}
        
    def build_insert(self):
        insert = self.INSERT % ({'table': self._table, 'args': ', '.join(self._insert_keys), \
                                 'values': ', '.join(['%s' for k in self._insert_values]), 'returning': self._returning})
        return {'sql': insert, 'parameters': self._insert_values, 'tables': [], 'columns': []}
        
    def build_update(self):
        update = self.UPDATE % ({'table': self._table, 'values': self._update, 'where': self._where, \
                                 'returning': self._returning})
        return {'sql': update, 'parameters': self._update_values + self._where_values, 'tables': [], 'columns': []}
        
    def _escape(self, name):
        return '"%s"' % name
        
