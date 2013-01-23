from manager import Manager
import settings

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
        if settings.STRICT:
            pks = Structure.get_primary_keys(table)
            try:
                return pks[0]
            except IndexError:
                return None
        else:
            return settings.PK_NAMING
    
    @staticmethod
    def get_foreign_keys(table, foreign_table):
        return Manager.get_scheme()[table]['fks'][foreign_table]
    
    @staticmethod
    def get_foreign_key(table, foreign_table):
        if settings.STRICT:
            fks = Structure.get_foreign_keys(table, foreign_table)
            try:
                return fks[0]
            except IndexError:
                return None
        else:
            return settings.FK_NAMING % foreign_table
            
