
from table import Table
from exception import DBException
from manager import Manager
from structure import Structure

class DB(object):
    
    def __init__(self, conn):
        Manager.set_connection(conn)
        Manager.renew_cache()
    
    def __getattr__(self, name):
        if Structure.table_exists(name):
            return Table(name)
        else:
            raise DBException('No table "%s" in database.' % name)
            
    def __dir__(self):
        return Structure.get_all_tables()
        

