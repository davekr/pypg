
from table import Table
from exception import DBException
from manager import Manager
from structure import Structure
import settings as dbsettings

class DB(object):
    
    def __init__(self, conn, settings=None):
        Manager.set_connection(conn)
        Manager.renew_cache()
        if settings:
            self._set_settings(settings)
    
    def __getattr__(self, name):
        if Structure.table_exists(name):
            return Table(name)
        else:
            raise DBException('No table "%s" in database.' % name)
            
    def __dir__(self):
        return Structure.get_all_tables()
        
    def _set_settings(self, settings):
        for setting in settings.availible_for_setting():
            value = getattr(settings, setting)
            if value is not None:
                setattr(dbsettings, setting, value)
        
        
class Settings(object):

    def __init__(self, **kwargs):
        for setting in self.availible_for_setting():
            setattr(self, setting, kwargs.get(setting.lower()))
        
    def __dir__(self):
        return self.availible_for_setting()
    
    def availible_for_setting(self):
        return ['DEBUG', 'SILENT', 'STRICT', 'PK_NAMING', 'FK_NAMING']
    
        
    
