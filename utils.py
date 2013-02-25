
from structure import Structure
from exception import DBException

class TableValidator(object):

    _NO_ARGUMENTS_ERROR = 'Method "%s" got no arguments.'

    def __init__(self, table_name):
        self._table_name = table_name
            
    def _check_column_in_table(self, column):
        return Structure.table_has_column(self._table_name, str(column))
            
    def _check_limit(self, limit):
        try:
            return int(limit)
        except ValueError:
            error = 'Wrong limit "%s". Limit has to be an integer or convertable to integer.' \
                    % limit
            raise DBException(error)
            
    def _validate_row(self, kwargs):
        self._validate_kwargs(kwargs, 'row')
        
    def _validate_update(self, kwargs):
        self._validate_kwargs(kwargs, 'update')
        
    def _validate_where(self, args, kwargs):
        self._validate_args_kwargs(args, kwargs, 'where', 'Literal')
        
    def _validate_insert(self, args, kwargs):
        self._validate_args_kwargs(args, kwargs, 'insert', 'Row')
    
    def _validate_kwargs(self, kwargs, method):
        if not kwargs:
            raise DBException(self._NO_ARGUMENTS_ERROR % method)
        map(self._check_column_in_table, kwargs.keys())
        
    def _validate_args_kwargs(self, args, kwargs, method, class_):
        if not args and not kwargs:
            raise DBException(self._NO_ARGUMENTS_ERROR % method)
        map(lambda arg: self._check_is_instance(arg, class_), args)
        map(self._check_column_in_table, kwargs.keys())
        
    def _check_is_instance(self, instance, class_):
        if not instance.__class__.__name__ == class_:
            raise DBException('Argument "%s" is not "%s" instance.' % (instance, class_))
        return True

