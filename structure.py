from manager import Manager

class Structure(object):

    @staticmethod
    def table_has_column(table, column):
        return column in Manager.get_scheme()[table]['columns']
        
    @staticmethod
    def get_all_tables():
        return Manager.get_scheme().keys()
        
    @staticmethod
    def table_exists(table):
        return table in Manager.get_scheme()
        
    @staticmethod
    def get_all_columns(table):
        return Manager.get_scheme()[table]['columns']
        
    @staticmethod
    def get_primary_keys(table):
        return Manager.get_scheme()[table]['pks']

    @staticmethod
    def get_primary_key(table):
        pks = Structure.get_primary_keys(table)
        try:
            return pks[0]
        except IndexError:
            return None
