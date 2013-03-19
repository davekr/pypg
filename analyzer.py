import json
from manager import Manager
import os
import subprocess
from collections import defaultdict
import psycopg2
from mview import MaterializedView
from table import TableSelect
from column import Column
import settings
from cursor import DALCursor
import time

class Analyzer(object):

    def __init__(self):
        self._modifying_time = defaultdict(lambda: 0)
        self._queries_to_improve = ImprovementCollection()
        self._all_queries = []

    def analyze(self):
        data = self.load_statistics()
        self.structure_statistics(data)
        self.filter_improvements()
        #self.report_stats()
        return self._queries_to_improve, self._all_queries

    def load_statistics(self):
        data = []
        with open('log/statistics.log') as stats:
            for line in stats.readlines():
                data.append(json.loads(line))
        return data

    def structure_statistics(self, data):
        for line in data:
            self._all_queries.append({'sql': line['query'], 'tables': line['tables']})
            if len(line['tables']) > 1:
                self._queries_to_improve.add_query(line['query'], line['time'], line['tables'], line['columns'])
            elif line['tables']:
                if not line['query'].startswith('SELECT'):
                    table = line['tables'][0]
                    self._modifying_time[table] += line['time']

    def filter_improvements(self):
        self._queries_to_improve.filter(self._modifying_time)

    def report_stats(self):
        import pprint
        pprint.pprint(self._modifying_time)
        print str(self._queries_to_improve)

class Improvement(object):

    def __init__(self, query, time, tables, columns):
        self.query = query
        self.time_read = time
        self.tables = tables
        self.columns = columns
        self.time_modify = 0
        self.improved_time_read = 0
        self.improved_time_modify = 0

    def add_read_time(self, time):
        self.time_read += time

    def improvement_sql(self):
        return 'SELECT * FROM test_mview;'

    def __str__(self):
        return 'Improvement: \n\tquery: %s\n\ttotal time reading: %s\n\ttotal time modifying: %s\n\timproved read: %s\n\timproved modify: %s' % (self.query, self.time_read, self.time_modify, self.improved_time_read, self.improved_time_modify)

    def run_improvement(self):
        self._improvement_mview()
        #result = self._check_column_denormalization()
        #if result:
            #self._improvement_add_column(*result)
        #else:
            #self._improvement_mview()

    def _check_column_denormalization():
        if len(self.tables) == 2:
            columns = [column for column in self.columns if column.startswith(self.tables[0])]
            columns2 = [column for column in self.columns if column.startswith(self.tables[1])]
            if len(columns == 1):
                return self.tables[1], self.tables[0], columns[0]
            if len(columns2 == 1):
                return self.tables[0], self.tables[1], columns[0]
        return None

    def _improvement_mview(self):
        MaterializedView().create_mview('test_mview', TableSelect(self.tables[0], \
                                        self.BuilderMockObject(self.query, self.tables, self.columns)))

    def _improvement_add_column(self, to_table, from_table, column):
        pass

    class BuilderMockObject(object):

        def __init__(self, query, tables, columns):
            self._query = query
            self._select_args = self._parse_columns(columns)
            self._tables = tables

        def add_select_arg(self, arg):
            self._select_args.append(arg)

        def add_aliases_to_select_args(self):
            self._select_args = ['%s AS %s' % (arg, arg.get_aliased_name()) for arg in self._select_args]

        def _parse_columns(self, columns):
            ret = []
            for column in columns:
                table, col = column.split('.')
                ret.append(Column(table, col))
            return ret

        def build_select(self):
            tail = self._query.split(' FROM ')[1]
            query = 'SELECT %s FROM %s' % (','.join(map(str, self._select_args)), tail)
            return {'sql': query, 'parameters': []}

class ImprovementCollection(object):

    def __init__(self):
        self.collection = []

    def add_query(self, query, time, *args, **kwargs):
        q = self[query]
        if q:
            q.add_read_time(time)
        else:
            self.collection.append(Improvement(query, time, *args, **kwargs))

    def filter(self, tables_modifying_time):
        collection = self.collection[:]
        for improvement in collection:
            total_modifying_time = 0
            for table in improvement.tables:
                total_modifying_time += tables_modifying_time[table]
            if total_modifying_time > improvement.time_read:
                self.collection.remove(improvement)
            else:
                idx = self.collection.index(improvement)
                self.collection[idx].time_modify = total_modifying_time

    def __contains__(self, item):
        return item in [improvement.query for improvement in self.collection]

    def __iter__(self):
        return iter(self.collection)

    def __getitem__(self, item):
        for improvement in self.collection:
            if item == improvement.query:
                return improvement

    def __len__(self):
        return len(self.collection)

    def __str__(self):
        return '\n'.join(map(str, self.collection))

class Denormalization(object):

    DBNAME = 'testingdatabase'

    def __init__(self):
        self.dbbackup = DBBackup()

    def start(self):
        settings.LOG = False
        improvements, all_queries = Analyzer().analyze()
        if not len(improvements):
            print 'No improvements found.'
            return None
        for improvement in improvements:
            self.set_up_enviroment()
            self.run_test(improvement, all_queries)
            self.tear_down_enviroment()
        self.report(improvements)

    def set_up_enviroment(self):
        print 'Setting up test enviroment'
        self.create_test_db()
        self.restore_db()

    def report(self, improvements):
        print str(improvements)

    def run_test(self, improvement, all_queries):
        print 'Testing improvement %s' % str(improvement)
        improvement.run_improvement()
        conn = Manager.get_connection()
        for query in all_queries:
            cursor = conn.cursor(cursor_factory=DALCursor)
            if query['sql'] == improvement.query:
                cursor.execute(improvement.improvement_sql())
                t = (time.time() - cursor.timestamp) * 1000
                improvement.improved_time_read += t
            elif len(query['tables']) == 1 and not query['sql'].startswith('SELECT'):
                cursor.execute(query['sql'])
                if query['tables'][0] in improvement.tables:
                    t = (time.time() - cursor.timestamp) * 1000
                    improvement.improved_time_modify += t
            cursor.close()
            conn.commit()

    def tear_down_enviroment(self):
        print 'Tearing down test enviroment'
        conn = Manager.get_connection()
        conn.close()
        cursor = self.orig_conn.cursor()
        cursor.execute("DROP DATABASE %s" % self.DBNAME)
        cursor.close()
        self.orig_conn.commit()
        Manager.set_connection(self.orig_conn)

    def create_test_db(self):
        print 'Creating db %s ' % self.DBNAME
        conn = Manager.get_connection()
        level = conn.isolation_level
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE %s" % self.DBNAME)
        cursor.close()
        conn.commit()
        self.orig_conn = conn
        parameters = self.dbbackup.parse_connection_dsn(conn.dsn)
        del parameters['dbname']
        parameters['database'] = self.DBNAME
        conn = psycopg2.connect(**parameters)
        Manager.set_connection(conn)

    def restore_db(self):
        print 'Restoring from backup'
        self.dbbackup.restore_backup()

class DBBackup(object):

    def __init__(self):
        self.command = []

    def create_backup(self):
        self.delete_log_and_dump()
        conn = Manager.get_connection()
        conn_parameters = self.parse_connection_dsn(conn.dsn)
        command = self.get_dump_command(conn_parameters)
        self.call_command(command)

    def restore_backup(self):
        if not self.command:
            conn = Manager.get_connection()
            conn_parameters = self.parse_connection_dsn(conn.dsn)
            self.command = self.get_restore_command(conn_parameters)
        self.call_command(self.command)

    def delete_log_and_dump(self):
        if os.path.exists('log/statistics.log'):
            os.remove('log/statistics.log')
        if os.path.exists('log/backup.sql'):
            os.remove('log/backup.sql')

    def parse_connection_dsn(self, dsn):
        return dict([arg.split('=') for arg in dsn.split(' ')])

    def get_dump_command(self, args):
        command = ['pg_dump']
        if args.has_key('host'):
            command.extend(['-h', args['host']])
        if args.has_key('port'):
            command.extend(['-p', args['port']])
        if args.has_key('user'):
            command.extend(['-U', args['user']])
        if args.has_key('password'):
            os.putenv('PGPASSWORD', args['password'])
        command.extend(['-f', 'log/backup.sql', args['dbname']])
        return command

    def get_restore_command(self, args):
        command = ['psql']
        if args.has_key('host'):
            command.extend(['-h', args['host']])
        if args.has_key('port'):
            command.extend(['-p', args['port']])
        if args.has_key('user'):
            command.extend(['-U', args['user']])
        if args.has_key('password'):
            os.putenv('PGPASSWORD', args['password'])
        command.extend(['-d', args['dbname'], '-f', 'log/backup.sql'])
        return command

    def call_command(self, command):
        subprocess.call(command, stdout=subprocess.PIPE)

