
from table import Table, TableSelect
from exception import DBException
from manager import Manager
from structure import Structure
from query import Query
from column import Column
import settings as dbsettings

class DB(object):
    
    def __init__(self, conn, settings=None):
        if settings:
            self._set_settings(settings)
        Manager.set_connection(conn)
        Manager.renew_cache()
    
    def __getattr__(self, name):
        Structure.table_exists(name)
        return Table(name)
            
    def __dir__(self):
        return Structure.get_all_tables()
        
    def _set_settings(self, settings):
        for setting in settings.availible_for_setting():
            value = getattr(settings, setting)
            if value is not None:
                setattr(dbsettings, setting, value)
        
    update_trigger_sql = """CREATE FUNCTION %(view)s_%(mview)s_%(table)s_ut() RETURNS TRIGGER
        SECURITY DEFINER LANGUAGE 'plpgsql' AS '
        BEGIN
          IF OLD.%(pk)s = NEW.%(pk)s THEN
            DELETE FROM %(mview)s WHERE %(pk)s = NEW.%(pk)s;
            INSERT INTO %(mview)s SELECT * FROM %(view)s WHERE %(pk)s = NEW.%(pk)s;
          ELSE
            DELETE FROM %(mview)s WHERE %(pk)s = OLD.%(pk)s;
            INSERT INTO %(mview)s SELECT * FROM %(view)s WHERE %(pk)s = NEW.%(pk)s;
          END IF;
          RETURN NULL;
        END
        ';
        CREATE TRIGGER %(view)s_%(mview)s_ut AFTER UPDATE ON %(table)s
          FOR EACH ROW EXECUTE PROCEDURE %(view)s_%(mview)s_%(table)s_ut();"""

    insert_trigger_sql = """CREATE FUNCTION %(view)s_%(mview)s_%(table)s_it() RETURNS TRIGGER
        SECURITY DEFINER LANGUAGE 'plpgsql' AS '
        BEGIN
          INSERT INTO %(mview)s SELECT * FROM %(view)s WHERE %(pk)s = NEW.%(pk)s;
          RETURN NULL;
        END
        ';
        CREATE TRIGGER %(view)s_%(mview)s_it AFTER INSERT ON %(table)s
          FOR EACH ROW EXECUTE PROCEDURE %(view)s_%(mview)s_%(table)s_it();"""

    delete_trigger_sql = """CREATE FUNCTION %(view)s_%(mview)s_%(table)s_dt() RETURNS TRIGGER
        SECURITY DEFINER LANGUAGE 'plpgsql' AS '
        BEGIN
          DELETE FROM %(mview)s WHERE %(pk)s = OLD.%(pk)s;
          RETURN NULL;
        END
        ';
        CREATE TRIGGER %(view)s_%(mview)s_dt AFTER DELETE ON %(table)s
          FOR EACH ROW EXECUTE PROCEDURE %(view)s_%(mview)s_%(table)s_dt();"""

    def create_mview(self, name, table):
        if not isinstance(table, TableSelect):
            raise DBExpcetion("Raw sql not yet supported. Use DAL syntax to create mview")
        pks = []
        for table_name in table._sql._tables:
            pk = Structure.get_primary_key(table_name)
            column = Column(table_name, pk)
            pks.append(pk)
            if not str(column) in table._sql._select_args:
                table._sql.add_select_arg(column)
        sql, values = table._sql.build_select()
        view_sql = "CREATE VIEW %s_view AS %s" % (name, sql)
        Query().execute(view_sql, values)
        mview_sql = 'CREATE TABLE %s AS SELECT * FROM %s;' % (name, name + "_view")
        Query().execute(mview_sql)
        pkeys_sql = 'ALTER TABLE %s ADD PRIMARY KEY (%s);' % (name, ','.join(['"%s"' % pk for pk in pks]))
        Query().execute(pkeys_sql)

        for table_name in table._sql._tables:
            pk = Structure.get_primary_key(table_name)
            Query().execute(self.update_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk}))
            Query().execute(self.insert_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk}))
            Query().execute(self.delete_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk}))
        
class Settings(object):

    def __init__(self, **kwargs):
        for setting in self.availible_for_setting():
            setattr(self, setting, kwargs.get(setting.lower()))
        
    def __dir__(self):
        return self.availible_for_setting()
    
    def availible_for_setting(self):
        return ['DEBUG', 'SILENT', 'STRICT', 'PK_NAMING', 'FK_NAMING']
    
