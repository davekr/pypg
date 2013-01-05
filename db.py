
from table import Table
from exception import DBException
from manager import Manager
from structure import Structure
import settings as dbsettings

class DB(object):
    
    def __init__(self, conn, settings=None):
        Manager.set_connection(conn)
        Manager.renew_cache()
        if settings and settings.debug:
            dbsettings.DEBUG = True
    
    def __getattr__(self, name):
        if Structure.table_exists(name):
            return Table(name)
        else:
            raise DBException('No table "%s" in database.' % name)
            
    def __dir__(self):
        return Structure.get_all_tables()
        
        
class Settings(object):

    def __init__(self, **kwargs):
        if kwargs.get('debug'):
            self.debug = True
        else:
            self.debug = False        
    

