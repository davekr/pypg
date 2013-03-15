
from literal import Literal

class Column(object):

    def __init__(self, table_name, name):
        self._name = name
        self._table_name = table_name
        
    def __eq__(self, value):
        sql = '%s = %%s' % self
        return Literal(sql, self, value)
        
    def __ne__(self, value):
        sql = '%s <> %%s' % self
        return Literal(sql, self, value)
        
    def __gt__(self, value):
        sql = '%s > %%s' % self
        return Literal(sql, self, value)
        
    def __lt__(self, value):
        sql = '%s < %%s' % self
        return Literal(sql, self, value)
    
    def like(self, value):
        sql = '%s LIKE %%s' % self
        return Literal(sql, self, str(value))
    
    def in_(self, value):
        if type(value) == list or type(value) == tuple:
            sql = '%s IN (%s)' % (self, ', '.join(['%s' for item in value]))
        else:
            sql = '%s IN (%%s)' % self
        return Literal(sql, self, value)
        
    def __str__(self):
        return '%s.%s' % (self._table_name, self._name)

    def get_aliased_name(self):
        return '%s_%s' % (self._table_name, self._name)
 
