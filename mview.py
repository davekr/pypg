from column import Column
from structure import Structure
from query import Query
from table import TableSelect
from manager import Manager
import settings
from exception import PyPgException

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

    def __init__(self):
        self.conn = Manager.get_connection()

    def create_mview(self, name, table):
        self.table = table
        if not isinstance(table, TableSelect):
            raise PyPgExpcetion("Raw sql not yet supported. Use PyPg syntax to create mview")
        self._validate_name(name)
        pks = self._prepare_query()
        read_commited = 1
        self.conn.set_isolation_level(read_commited)
        try:
            self._make_mview(pks, name)
            self.conn.commit()
            print 'Materialized view %s created successfully' % name
        except Exception, e:
            self.conn.rollback()
            raise e

    def _validate_name(self, name):
        cursor = self.conn.cursor()
        cursor.execute("select (1) from information_schema.tables where table_name=%s", [name])
        exists = cursor.fetchall()
        cursor.close()
        self.conn.commit()
        if len(exists):
            raise PyPgException("Table with that name already exists")

    def _prepare_query(self):
        pks = []
        if not self.table._sql._select_args:
            if settings.STRICT == False:
                settings.STRICT = True
                Manager.renew_cache()
            for table_name in self.table._sql._tables:
                pk = Structure.get_primary_key(table_name)
                c = Column(table_name, pk)
                pks.append(c.get_aliased_name())
                for column in Structure.get_all_columns(table_name):
                    c = Column(table_name, column)
                    self.table._sql.add_select_arg(c)
        else:
            for table_name in self.table._sql._tables:
                pk = Structure.get_primary_key(table_name)
                column = Column(table_name, pk)
                pks.append(column.get_aliased_name())
                if not str(column) in map(str, self.table._sql._select_args):
                    self.table._sql.add_select_arg(column)
        self.table._sql.add_aliases_to_select_args()
        return pks

    def _make_mview(self, pks, name):
        cursor = self.conn.cursor()
        sql_dict = self.table._sql.build_select()
        sql, values = sql_dict['sql'], sql_dict['parameters']
        view_sql = "CREATE VIEW %s_view AS %s" % (name, sql)
        cursor.execute(view_sql, values)
        mview_sql = 'CREATE TABLE %s AS SELECT * FROM %s;' % (name, name + "_view")
        cursor.execute(mview_sql)
        pkeys_sql = 'ALTER TABLE %s ADD PRIMARY KEY (%s);' % (name, ','.join(['"%s"' % pk for pk in pks]))
        cursor.execute(pkeys_sql)

        for table_name in self.table._sql._tables:
            pk = Structure.get_primary_key(table_name)
            mview_pk = Column(table_name, pk).get_aliased_name()
            cursor.execute(self.update_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk, 'mview_pk': mview_pk}))
            cursor.execute(self.insert_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk, 'mview_pk': mview_pk}))
            cursor.execute(self.delete_trigger_sql % ({'view': name + '_view', 'mview': name, \
                                                        'table': table_name, 'pk': pk, 'mview_pk': mview_pk}))
        Manager.renew_cache()

    def drop_mview(self, name):
        tables = self._get_all_involved_tables(name)
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
        print 'Materialized view %s dropped successfully' % name

    def _get_all_involved_tables(self, name):
        sql = 'SELECT relname FROM pg_trigger JOIN pg_class ON tgrelid=pg_class.oid WHERE tgname=%s;'
        triggername = '%(name)s_view_%(name)s_ut' % {'name': name}
        data = Query().execute_and_fetch(sql, parameters=[triggername])
        return [row['relname'] for row in data]

