from collections import defaultdict
from exception import DBException

class Manager(object):

    _CONNECTION = None
    _INTROSPECTION_CACHE = defaultdict(list)
    _CACHE_POPULATED = False
    
    @staticmethod
    def set_connection(conn):
        Manager._CONNECTION = conn
        
    @staticmethod
    def get_connection():
        if Manager._CONNECTION:
            return Manager._CONNECTION
        else:
            raise DBException('No connection specified.')
            
    @staticmethod
    def get_scheme():
        if not Manager._CACHE_POPULATED:
            raise DBException('Cache is old or has never been popuated. Please renew cache.')
        else:
            return Manager._INTROSPECTION_CACHE
                    
    @staticmethod
    def renew_cache():
        select = "SELECT t.table_name, c.column_name FROM information_schema.tables as t, information_schema.columns as c \
                  WHERE t.table_schema = 'public' AND c.table_name = t.table_name;"
        cursor = Manager._CONNECTION.cursor()
        cursor.execute(select)
        Manager._populate_cache(cursor.fetchall())
        cursor.close()
    
    @staticmethod
    def _populate_cache(data):
        Manager._CACHE_POPULATED = True
        Manager._INTROSPECTION_CACHE.clear()
        for table_name, column_name in data:
            Manager._INTROSPECTION_CACHE[table_name].append(column_name)
        
