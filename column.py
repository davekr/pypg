
from literal import Literal

class Column(object):

    def __init__(self, name):
        self._name = name
        
    def __eq__(self, value):
        sql = '"%s" = %s' % (self._name, '%s')
        return Literal(sql, value)
        
    def __ne__(self, value):
        sql = '"%s" <> %s' % (self._name, '%s')
        return Literal(sql, value)
        
    def __gt__(self, value):
        sql = '"%s" > %s' % (self._name, '%s')
        return Literal(sql, value)
        
    def __lt__(self, value):
        sql = '"%s" < %s' % (self._name, '%s')
        return Literal(sql, value)
    
    def like(self, value):
        sql = '"%s" LIKE %s' % (self._name, '%s')
        return Literal(sql, str(value))
    
    def in_(self, value):
        if type(value) == list or type(value) == tuple:
            sql = '"%s" IN (%s)' % (self._name, ', '.join(['%s' for item in value]))
        else:
            sql = '"%s" IN (%s)' % (self._name, '%s')
        return Literal(sql, value)
        
    def __str__(self):
        return self._name
 
    
    
