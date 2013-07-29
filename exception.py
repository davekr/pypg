# -*- coding: utf-8 -*- 

class PyPgException(Exception):
    """Třída vytvořená pro výjimky vyvolané knihovnou."""
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)
