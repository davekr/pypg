from query import Query

class AddColumn(object):

    def run(self, to_table, from_table, column, on_condition):
        ctype = self.get_column_type(from_table, column)
        self.add_column(to_table, column, ctype)
        self.populate_column(to_table, from_table, column, on_condition)
        self.create_triggers(to_table, from_table, column, on_condition)

    def get_column_type(self, table, column):
        q = 'select data_type from information_schema.columns where table_name = %s and column_name= %s;'
        result = Query().execute_and_fetch(q, parameters = (table, column))
        return result[0][0]

    def add_column(self, table, column, ctype):
        q = 'ALTER TABLE "%s" ADD COLUMN "%s" %s;' % (table, column, ctype)
        Query().execute(q)

    def populate_column(self, to_table, from_table, column, on):
        q = 'update "%(to_table)s" set "%(column)s"= %(from_table)s.%(column)s from "%(from_table)s" where %(on)s;' \
                % locals()
        Query().execute(q)

    def create_triggers(self, to_table, from_table, column, on_condition):
    update_trigger_sql = """CREATE FUNCTION %(column)s_%(table)s_fnc_ut() RETURNS TRIGGER
        SECURITY DEFINER LANGUAGE 'plpgsql' AS '
        BEGIN
          update %(table)s set %(column)s= %(from_table)s.%(column)s from %(from_table)s where %(on)s; 
          RETURN NULL;
        END
        ';
        CREATE TRIGGER %(column)s_%(table)s_ut AFTER UPDATE ON %(table)s
          FOR EACH ROW EXECUTE PROCEDURE %(column)s_%(table)s_fnc_ut();"""
    #podminka on musi byt lepe zparsovana a musi tam prijit NEW.value


