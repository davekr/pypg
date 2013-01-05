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
