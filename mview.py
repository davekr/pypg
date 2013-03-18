from column import Column
from structure import Structure
from query import Query
from table import TableSelect
from manager import Manager
import settings

class MaterializedView(object):

    update_trigger_sql = """CREATE FUNCTION %(view)s_%(mview)s_%(table)s_ut() RETURNS TRIGGER
        SECURITY DEFINER LANGUAGE 'plpgsql' AS '
        BEGIN
          IF OLD.%(pk)s = NEW.%(pk)s THEN
            DELETE FROM %(mview)s WHERE %(mview_pk)s = NEW.%(pk)s;
            INSERT INTO %(mview)s SELECT * FROM %(view)s WHERE %(mview_pk)s = NEW.%(pk)s;
          ELSE
            DELETE FROM %(mview)s WHERE %(mview_pk)s = OLD.%(pk)s;
            INSERT INTO %(mview)s SELECT * FROM %(view)s WHERE %(mview_pk)s = NEW.%(pk)s;
          END IF;
          RETURN NULL;
        END
        ';
        CREATE TRIGGER %(view)s_%(mview)s_ut AFTER UPDATE ON %(table)s
          FOR EACH ROW EXECUTE PROCEDURE %(view)s_%(mview)s_%(table)s_ut();"""

    insert_trigger_sql = """CREATE FUNCTION %(view)s_%(mview)s_%(table)s_it() RETURNS TRIGGER
        SECURITY DEFINER LANGUAGE 'plpgsql' AS '
        BEGIN
          INSERT INTO %(mview)s SELECT * FROM %(view)s WHERE %(mview_pk)s = NEW.%(pk)s;
          RETURN NULL;
        END
        ';
        CREATE TRIGGER %(view)s_%(mview)s_it AFTER INSERT ON %(table)s
          FOR EACH ROW EXECUTE PROCEDURE %(view)s_%(mview)s_%(table)s_it();"""

    delete_trigger_sql = """CREATE FUNCTION %(view)s_%(mview)s_%(table)s_dt() RETURNS TRIGGER
        SECURITY DEFINER LANGUAGE 'plpgsql' AS '
        BEGIN
          DELETE FROM %(mview)s WHERE %(mview_pk)s = OLD.%(pk)s;
          RETURN NULL;
        END
        ';
        CREATE TRIGGER %(view)s_%(mview)s_dt AFTER DELETE ON %(table)s
          FOR EACH ROW EXECUTE PROCEDURE %(view)s_%(mview)s_%(table)s_dt();"""

    drop_update_trigger_sql = """DROP TRIGGER %(view)s_%(mview)s_ut ON %(table)s;
        DROP FUNCTION %(view)s_%(mview)s_%(table)s_ut();
        """
    
    drop_insert_trigger_sql = """DROP TRIGGER %(view)s_%(mview)s_it ON %(table)s;
        DROP FUNCTION %(view)s_%(mview)s_%(table)s_it();
        """

    drop_delete_trigger_sql = """DROP TRIGGER %(view)s_%(mview)s_dt ON %(table)s;
        DROP FUNCTION %(view)s_%(mview)s_%(table)s_dt();
        """

    def create_mview(self, name, table):
        if not isinstance(table, TableSelect):
            raise DBExpcetion("Raw sql not yet supported. Use DAL syntax to create mview")
        pks = []
        if not table._sql._select_args:
            if settings.STRICT == False:
                settings.STRICT = True
                Manager.renew_cache()
            for table_name in table._sql._tables:
                pk = Structure.get_primary_key(table_name)
                c = Column(table_name, pk)
                pks.append(c.get_aliased_name())
                for column in Structure.get_all_columns(table_name):
                    c = Column(table_name, column)
                    table._sql.add_select_arg(c)
        else:
            for table_name in table._sql._tables:
                pk = Structure.get_primary_key(table_name)
                column = Column(table_name, pk)
                pks.append(column.get_aliased_name())
                if not str(column) in map(str, table._sql._select_args):
                    table._sql.add_select_arg(column)
        table._sql.add_aliases_to_select_args()
        sql_dict = table._sql.build_select()
        sql, values = sql_dict['sql'], sql_dict['parameters']
        view_sql = "CREATE VIEW %s_view AS %s" % (name, sql)
        Query().execute(view_sql, paramters=values)
        mview_sql = 'CREATE TABLE %s AS SELECT * FROM %s;' % (name, name + "_view")
        Query().execute(mview_sql)
        pkeys_sql = 'ALTER TABLE %s ADD PRIMARY KEY (%s);' % (name, ','.join(['"%s"' % pk for pk in pks]))
        Query().execute(pkeys_sql)

        for table_name in table._sql._tables:
            pk = Structure.get_primary_key(table_name)
            mview_pk = Column(table_name, pk).get_aliased_name()
            Query().execute(self.update_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk, 'mview_pk': mview_pk}))
            Query().execute(self.insert_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk, 'mview_pk': mview_pk}))
            Query().execute(self.delete_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk, 'mview_pk': mview_pk}))
        Manager.renew_cache()

    def drop_mview(self, name):
        tables = self.get_all_involved_tables(name)
        for table in tables:
            Query().execute(self.drop_update_trigger_sql % {'view': name + '_view', 'mview': name, \
                                                            'table': table})
            Query().execute(self.drop_insert_trigger_sql % {'view': name + '_view', 'mview': name, \
                                                            'table': table})
            Query().execute(self.drop_delete_trigger_sql % {'view': name + '_view', 'mview': name, \
                                                            'table': table})
        if tables:
            view_sql = 'DROP VIEW %(view)s;' % {'view': name + '_view'}
            mview_sql = 'DROP TABLE %(mview)s;' % {'mview': name}
            Query().execute(mview_sql)
            Query().execute(view_sql)

    def get_all_involved_tables(self, name):
        sql = 'SELECT relname FROM pg_trigger JOIN pg_class ON tgrelid=pg_class.oid WHERE tgname=%s;'
        triggername = '%(name)s_view_%(name)s_ut' % {'name': name}
        data = Query().execute_and_fetch(sql, parameters=[triggername])
        return [row['relname'] for row in data]

