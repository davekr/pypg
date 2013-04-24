from table import TableSelect, TableSelected
from exception import PyPgException

class RestrictedTableSelect(TableSelect):

    def __init__(self, name, sql, result_set):
        super(RestrictedTableSelect, self).__init__(name, sql)
        self._result_set = result_set

    def join(self, *args, **kwargs):
        raise PyPgException("Join not supported in SELECT...IN functionality")

    def _table_select_instance(self):
        return self
        
    def _table_selected_instance(self):
        return RestrictedTableSelected(self._table_name, self._sql, self._result_set)

    def _table_where_instance(self):
        return self
    
class RestrictedTableSelected(TableSelected):

    def __init__(self, name, sql, result_set):
        super(RestrictedTableSelected, self).__init__(name, sql)
        self._result_set = result_set

    def _get_data(self):
        return self._result_set._get_rel_data_restricted(self._sql)
