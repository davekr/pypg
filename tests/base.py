import psycopg2
from pypg import PyPg
import subprocess
        
class TestHelper(object):

    DBNAME = "test_world"
    DBUSER = "dave"

    @classmethod
    def setUp(cls):
        cls.create_db()
        cls.fill_db()
        cls.conn = psycopg2.connect("dbname=%s user=%s" % (cls.DBNAME, cls.DBUSER))
        cls.db = PyPg(cls.conn)

    @classmethod
    def fill_db(cls):
        print "Filling test database %s." % cls.DBNAME
        import os
        dirname = os.path.dirname(os.path.abspath(__file__))
        subprocess.call(["psql", "-d", cls.DBNAME, "-U", cls.DBUSER, "-f", "%s/%s.sql" % (dirname, cls.DBNAME)], \
                        stdout=subprocess.PIPE)

    @classmethod
    def create_db(cls):
        print "Creating test database %s." % cls.DBNAME
        conn = psycopg2.connect("dbname=postgres user=%s" % cls.DBUSER)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE %s" % cls.DBNAME)
        cursor.close()
        conn.commit()
        conn.close()

    @classmethod
    def tearDown(cls):
        cls.conn.close()
        cls.drop_db()

    @classmethod
    def drop_db(cls):
        print "\nDropping test database %s." % cls.DBNAME
        conn = psycopg2.connect("dbname=postgres user=%s" % cls.DBUSER)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE %s" % cls.DBNAME)
        cursor.close()
        conn.commit()
        conn.close()
