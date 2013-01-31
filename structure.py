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
        #if settings.STRICT:
        pks = Structure.get_primary_keys(table)
        try:
            return pks[0]
        except IndexError:
            return None
        #else:
        #    return settings.PK_NAMING

    @staticmethod
    def get_foreign_keys(table):
        try:
            return reduce(lambda x, y: x + y, map(lambda x: x['relcolumns'], Manager.get_scheme()[table]['fks'].values()))
        except TypeError:
            return []
        
    @staticmethod
    def tables_related(table, reltable):
        return reltable in Manager.get_scheme()[table]['fks'] or table in Manager.get_scheme()[reltable]['fks']
    
    @staticmethod
    def get_foreign_keys_for_table(table, foreign_table):
        return Manager.get_scheme()[table]['fks'][foreign_table]['relcolumns']
    
    @staticmethod
    def get_foreign_key_for_table(table, foreign_table):
        if settings.STRICT:
            fks = Structure.get_foreign_keys(table, foreign_table)
            try:
                return fks[0]
            except IndexError:
                return None
        else:
            return settings.FK_NAMING % foreign_table
           
    @staticmethod
    def get_fk_referenced_table(table, foreign_key):
        for table, fks in Manager.get_scheme()[table]['fks'].items():
            if foreign_key in fks['relcolumns']:
                return table
                
    @staticmethod
    def get_reltable_fks_for_table(table, reltable):
        return Manager.get_scheme()[reltable]['fks'][table]['relcolumns']
        
    @staticmethod
    def get_reltable_fk_for_table(table, reltable):
        fks = Structure.get_reltable_fks_for_table(table, reltable)
        try:
            return fks[0]
        except IndexError:
            return None
    
                
            
