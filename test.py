import psycopg2
import unittest
from db import DB
from exception import DBException
from table import TableWhere, Table, TableSelect
from row import Row
from resultset import ResultSet

db = DB(psycopg2.connect("dbname=queens user=dave"))
        
    
class DBTest(unittest.TestCase):

    def test_table_access(self):
        self.assertEqual(isinstance(db.test, Table), True)
    
    def test_table_access_failure(self):
        self.assertRaises(DBException, getattr, db, 'notexistingtable')
        
class TableInsertTest(unittest.TestCase):

    def test_insert_no_arguments(self):
        self.assertRaises(DBException, db.test.insert)
        
    def test_insert_bad_karguments(self):
        self.assertRaises(DBException, db.test.insert, notexistingcolumn='')
        
    def test_insert_good_karguments(self):
        self.assertEqual(isinstance(db.test.insert(name='Test'), ResultSet), True)
        
    def test_insert_bad_arguments(self):
        self.assertRaises(DBException, db.test.insert, 'blabla')
        
    def test_insert_good_arguments(self):
        #self.assertEqual(isinstance(self.db.test.insert(self.db.test.row()), list), True)
        pass
        
class TableSelectTest(unittest.TestCase):

    def test_limit_no_arguments(self):
        self.assertRaises(TypeError, db.test.limit)
        
    def test_limit_bad_arguments(self):
        self.assertRaises(DBException, db.test.limit, 'blabla')
        
    def test_limit_good_arguments(self):
        self.assertEqual(isinstance(db.test.limit(10), TableSelect), True)
    
    def test_order_no_arguments(self):
        self.assertRaises(TypeError, db.test.order)
        
    def test_order_bad_arguments(self):
        self.assertRaises(DBException, db.test.order, 'notexistingcolumn')    
        
    def test_order_good_arguments(self):
        self.assertEqual(isinstance(db.test.order('id'), TableSelect), True)
        
    def test_order_good_arguments2(self):
        self.assertEqual(isinstance(db.test.order(db.test.id), TableSelect), True)    
        
    def test_select_no_arguments(self):
        self.assertEqual(isinstance(db.test.select(), ResultSet), True)
         
    def test_select_bad_arguments(self):
        self.assertRaises(DBException, db.test.select, 'notexistingcolumn')
        
    def test_select_good_arguments(self):
        self.assertEqual(isinstance(db.test.select('id'), ResultSet), True)
        
    def test_select_good_arguments2(self):
        self.assertEqual(isinstance(db.test.select(db.test.id), ResultSet), True)
        
class TableWhereTest(unittest.TestCase):

    def test_where_no_arguments(self):
        self.assertRaises(DBException, db.test.where)
    
    def test_where_bad_karguments(self):
        self.assertRaises(DBException, db.test.where, notexistingcolumn='')
        
    def test_where_bad_arguments(self):
        self.assertRaises(DBException, db.test.where, 'blabla')
        
    def test_where_good_karguments(self):
        self.assertEqual(isinstance(db.test.where(id=1), TableWhere), True)
        
    def test_where_good_arguments(self):
        self.assertEqual(isinstance(db.test.where(db.test.id.notequal('1')), TableWhere), True)
        
class RowTest(unittest.TestCase):

    def setUp(self):
        self.row = db.test.select()[0]
        
    def test_item_access(self):
        self.assertEqual(type(self.row['name']), str)
        
    def test_item_access_failure(self):
        def get_item():
            self.row['notexistningcolumn']
        self.assertRaises(KeyError, get_item)
        
    def test_update_no_arguments(self):
        self.assertRaises(DBException, self.row.update)
        
    def test_update_bad_arguments(self):
        self.assertRaises(DBException, self.row.update, notexistingcolumn='')
        
    def test_item_set_failuer(self):
        def set_item(x):
            self.row['notexistningcolumn'] = x
        self.assertRaises(DBException, set_item , '')

    def test_update_good_arguments(self):
        self.assertEqual(type(self.row.update(name='Test')), Row)
        
    def test_update_good_arguments2(self):
        self.row['name'] = 'Test'
        self.assertEqual(type(self.row.update()), Row)
        
class RowDeleteTest(unittest.TestCase):
    
    def setUp(self):
        self.row = db.test.select()[0]
        
    def test_delete(self):
        self.assertEqual(self.row.delete(), None)
        
class RowDeletedTest(unittest.TestCase):
    
    def setUp(self):
        self.row = db.test.select()[0]
        self.row.delete()
        
    def test_item_access_failure(self):
        def get_item():
            self.row['name']
        self.assertRaises(DBException, get_item)
        
    def test_item_set_failure(self):
        def set_item():
            self.row['name'] = 'Test'
        self.assertRaises(DBException, set_item)
        
    def test_update_failure(self):
        self.assertRaises(DBException, self.row.update, name='Test')
        
    def test_delete_failure(self):
        self.assertRaises(DBException, self.row.delete)
        
if __name__ == '__main__':
    unittest.main()
