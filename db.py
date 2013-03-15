
from table import Table, TableSelect
from exception import DBException
from manager import Manager
from structure import Structure
from mview import MaterializedView
import settings as dbsettings

class DB(object):
    
    def __init__(self, conn, logger=None, naming=None, **kwargs):
        Manager.set_connection(conn)
        self._set_settings(kwargs)
        Manager.set_logger(logger)
        Manager.set_naming(naming)
    
    def __getattr__(self, name):
        Structure.table_exists(name)
        return Table(name)
            
    def __dir__(self):
        attrs = ['create_mview', 'drop_view', 'set_debug', 'set_log', 'set_strict', 'set_naming', 'set_logger']
        return Structure.get_all_tables() + attrs
        
    def _set_settings(self, settings):
        for value in settings:
            func = getattr(self, "set_%s" % value, None)
            if func:
                func(settings[value])

    def create_mview(self, name, table):
        MaterializedView().create_mview(name, table)

    def drop_mview(self, name):
        MaterializedView().drop_mview(name)

    def set_debug(self, value):
        self._set('DEBUG', value)
        
    def set_log(self, value):
        self._set('LOG', value)

    def set_strict(self, value):
        if dbsettings.STRICT == False and value == True:
            self._set('STRICT', value)
            Manager.renew_cache()
        else:
            self._set('STRICT', value)

    def set_naming(self, value):
        Manager.set_naming(value)

    def set_logger(self, value):
        Manager.set_logger(value)

    def _set(self, setting, value):
        if value:
            setattr(dbsettings, setting, True)
        else:
            setattr(dbsettings, setting, False)
        
