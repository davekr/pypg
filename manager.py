from collections import defaultdict
from exception import PyPgException
import settings
import logging
import os

class Manager(object):

    _CONNECTION = None
    #Example entry: 'book': {'columns': ['title', 'id', 'author_id'], 'pks': ['id'], \
    #                           'fks': {'author': {'relcolumns': ['author_id'], 'columns': ['id']}}}
    _INTROSPECTION_CACHE = defaultdict(lambda: {'columns': [], 'pks': [], 'fks': \
                                defaultdict(lambda: {'relcolumns': [], 'columns': []})})
    _CACHE_POPULATED = False
    _INTROSPECTION_SELECT = """
                            SELECT t1.table_name, t1.column_name, t2.constraint_type, 
                                   t2.foreign_table_name, t2.foreign_column_name
                            FROM ( 
                                SELECT t.table_name, c.column_name 
                                FROM information_schema.tables as t, information_schema.columns as c
                                WHERE t.table_schema = 'public' AND c.table_name = t.table_name
                            ) AS t1
                            LEFT JOIN (
                                SELECT
                                constraint_type, kcu.column_name, tc.table_name,
                                ccu.table_name AS foreign_table_name,
                                ccu.column_name AS foreign_column_name 
                                FROM 
                                information_schema.table_constraints AS tc 
                                JOIN information_schema.key_column_usage AS kcu 
                                ON tc.constraint_name = kcu.constraint_name
                                JOIN information_schema.constraint_column_usage AS ccu 
                                ON ccu.constraint_name = tc.constraint_name
                                WHERE constraint_type = 'FOREIGN KEY' OR constraint_type = 'PRIMARY KEY'
                           ) AS t2 
                           ON t1.column_name = t2.column_name AND t1.table_name = t2.table_name;
                           """
    _NAMING = None

    _LIB_PATH = os.path.realpath(os.path.dirname(__file__))
    
    @staticmethod
    def set_connection(conn):
        Manager._CONNECTION = conn
        
    @staticmethod
    def get_connection():
        if Manager._CONNECTION:
            return Manager._CONNECTION
        else:
            raise PyPgException('No connection specified.')

    @staticmethod
    def get_logger():
        logger = Manager._LOGGER
        if settings.DEBUG:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)
        return logger

    @staticmethod
    def set_logger(logger):
        if not logger:
            logger = logging.getLogger("queryLogger")
            if not logger.handlers:
                logger.addHandler(logging.StreamHandler())
        Manager._LOGGER = logger

    @staticmethod
    def get_naming():
        return Manager._NAMING

    @staticmethod
    def set_naming(naming):
        from structure import Naming
        if naming: 
            if not isinstance(naming, Naming):
                raise PyPgException('Naming must be instance of class utils.Naming')
            else:
                Manager._NAMING = naming
        else:
            Manager._NAMING = Naming()
            
    @staticmethod
    def get_scheme():
        if not Manager._CACHE_POPULATED:
            raise PyPgException('Cache is old or has never been popuated. Please renew cache.')
        else:
            return Manager._INTROSPECTION_CACHE

    @staticmethod
    def get_path():
        return Manager._LIB_PATH
                    
    @staticmethod
    def renew_cache():
        if settings.STRICT:
            cursor = Manager._CONNECTION.cursor()
            cursor.execute(Manager._INTROSPECTION_SELECT)
            Manager._populate_cache(cursor.fetchall())
            cursor.close()
    
    @staticmethod
    def _populate_cache(data):
        Manager._CACHE_POPULATED = True
        Manager._INTROSPECTION_CACHE.clear()
        for row in data:
            table_name, column_name, key, fk_table, fk_pk = row
            Manager._INTROSPECTION_CACHE[table_name]['columns'].append(column_name)
            if key == 'PRIMARY KEY':
                Manager._INTROSPECTION_CACHE[table_name]['pks'].append(column_name)
            if key == 'FOREIGN KEY':
                Manager._INTROSPECTION_CACHE[table_name]['fks'][fk_table]['columns'].append(fk_pk)
                Manager._INTROSPECTION_CACHE[table_name]['fks'][fk_table]['relcolumns'].append(column_name)
                
        
