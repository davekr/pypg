# -*- coding: utf-8 -*-

class Literal(object):
    """Reprezentace podmínky v SQL dotazu obsahující text a hodnoty.
    Třída SQLBuilder z instancí této třídy vytváří SQL dotazy."""

    def __init__(self, sql, column, value):
        self._sql = sql
        self._value = value
        self._column = column
        
    def __str__(self):
        if type(self._value) == list:
            return self._sql % tuple(self._value)
        else:
            return self._sql % self._value
        
    def get(self):
        return self._sql, self._value
