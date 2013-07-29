# -*- coding: utf-8 -*-

class ResultSetCache(object):
    """Třída uchovává data při použití funkce samostatných dotazů. 
    Umožňuje v těchto datech vyhledávat a rozdělovat je podle příslušných vazeb.
    Je využívána třídou ResultSet"""
        
    def __init__(self, table, data):
        self._cache = {}        
        self._cache[table] = {}
        self._cache[table]['data'] = data
        
    def relation_exists(self, table, relation):
        if self._cache.get(table) and self._cache[table].get(relation):
            return True
        else:
            return False

    def get_all_keys(self, table, key_name):
        return [row[key_name] for row in self._cache[table]['data']]

    def save_relation(self, table, relation, data):
        if not self._cache.get(table):
            self._cache[table] = {}
        self._cache[table][relation] = True
        if not self._cache.get(relation):
            self._cache[relation] = {}
        self._cache[relation]['data'] = data

    def get_relation_row(self, relation, key, key_value):
        rows = self.get_relation_set(relation, key, key_value)
        return rows[0]

    def get_relation_set(self, relation, key, key_value):
        return [row for row in self._cache[relation]['data'] if row[key] == key_value]

    def __str__(self):
        import pprint
        pp = pprint.PrettyPrinter()
        pp.pprint(self._cache)
        return ''
