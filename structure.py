from manager import Manager
import settings
from exception import PyPgException

class Structure(object):

    @staticmethod
    def table_has_column(table, column):
        if settings.STRICT:
            if column in Manager.get_scheme()[table]['columns']:
                return True
            else:
                if settings.DEBUG:
                    columns = ' Choices are: %s' % ', '.join(Structure.get_all_columns(table))
                else:
                    columns = ''
                error = 'Column "%s" is not a valid column in table "%s".%s' \
                        % (column, table, columns)
                raise PyPgException(error)
        else:
            return True
        
    @staticmethod
    def get_all_tables():
        if settings.STRICT:
            return Manager.get_scheme().keys()
        else:
            return []
        
    @staticmethod
    def table_exists(table):
        if settings.STRICT:
            if table in Manager.get_scheme():
                return True
            else:
                if settings.DEBUG:
                    tables = ' Choices are: %s' % ', '.join(Structure.get_all_tables())
                else:
                    tables = ''
                raise PyPgException('No table "%s" in database.%s' % (table, tables))
        else:
            return True
        
    @staticmethod
    def get_all_columns(table):
        if settings.STRICT:
            return Manager.get_scheme()[table]['columns']
        else:
            return []
        
    @staticmethod
    def get_primary_keys(table):
        if settings.STRICT:
            return Manager.get_scheme()[table]['pks']
        else:
            return [Manager.get_naming().get_pk_naming(table)]

    @staticmethod
    def get_primary_key(table):
        try:
            return Structure.get_primary_keys(table)[0]
        except IndexError:
            raise PyPgException('Table %s has no primary key.' % table)

    @staticmethod
    def is_foreign_key(table, attr):
        if settings.STRICT:
            relcolumns = map(lambda x: x['relcolumns'], Manager.get_scheme()[table]['fks'].values())
            fks = reduce(lambda x, y: x + y, relcolumns, []) 
            return attr in fks
        else:
            return Manager.get_naming().match_fk_naming(table, attr)

    @staticmethod
    def tables_related(table, reltable):
        if settings.STRICT:
            if reltable in Manager.get_scheme()[table]['fks'] or table in Manager.get_scheme()[reltable]['fks']:
                return True
            else:
                raise PyPgException('Tables %s and %s are not related.' % (table, reltable))
        else:
            return True
    
    @staticmethod
    def get_foreign_keys_for_table(table, foreign_table):
        if settings.STRICT:
            return Manager.get_scheme()[table]['fks'][foreign_table]['relcolumns']
        else:
            return [Manager.get_naming().get_fk_naming(table, foreign_table)]
    
    @staticmethod
    def get_foreign_key_for_table(table, foreign_table):
        try:
            return Structure.get_foreign_keys_for_table(table, foreign_table)[0]
        except IndexError:
            raise PyPgException('Table %s has no foreign key for table %s' % (table, foreign_table))
           
    @staticmethod
    def get_fk_referenced_table(table, foreign_key):
        if settings.STRICT:
            for table, fks in Manager.get_scheme()[table]['fks'].items():
                if foreign_key in fks['relcolumns']:
                    return table
            else:
                raise PyPgException('Table %s has no foreign key %s.' % (table, foreign_key))
        else:
            return Manager.get_naming().get_fk_column(table, foreign_key)
                
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
    
class Naming(object):

    def get_pk_naming(self, table):
        return "id"

    def get_fk_naming(self, table, foreign_table):
        return "%s_id" % foreign_table

    def match_fk_naming(self, table, attr):
        return attr.endswith("_id")

    def get_fk_column(self, table, foreign_key):
        return foreign_key.rstrip("_id")
                
