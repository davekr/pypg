
class ResultSet(object):

    def __init__(self, data):
        self._data = data
        
    def __str__(self):
        return self.pretty_print()
    
    def __getitem__(self, item):
        return self._data[item]
        
    def __len__(self):
        return len(self._data)
        
    def pretty_print(self):
        return '\n'.join([str(row) for row in self._data])
        
