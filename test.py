import unittest
from exception import DBException
from table import TableWhere, Table, TableSelect, TableSelected
from column import Column
from row import Row
from resultset import ResultSet
from tests import TestHelper

class DBTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(False)
    
    def test_notexisting_table_access(self):
        self.assertEqual(isinstance(self.db.notexistingtable, Table), True)

class DBStrictTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def test_table_access(self):
        self.assertEqual(isinstance(self.db.city, Table), True)
    
    def test_table_access_failure(self):
        self.assertRaises(DBException, getattr, self.db, 'notexistingtable')

class TableInsertTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def test_insert_get_data_failure(self):
        import operator
        self.assertRaises(TypeError, operator.getitem, self.db.city, 0)

    def test_insert_no_arguments(self):
        self.assertRaises(DBException, self.db.city.insert)

    def test_insert_and_get_no_arguments(self):
        self.assertRaises(DBException, self.db.city.insert_and_get)
    
    def test_insert_bad_karguments(self):
        self.assertRaises(DBException, self.db.city.insert, notexistingcolumn='')

    def test_insert_and_get_bad_karguments(self):
        self.assertRaises(DBException, self.db.city.insert_and_get, notexistingcolumn='')
         
    def test_insert_good_karguments(self):
        self.assertEqual(self.db.city.insert(name='Test', countrycode="CZE", district="Testdistrict", population=300), None)

    def test_insert_and_get_good_karguments(self):
        self.assertEqual(isinstance(self.db.city.insert_and_get(name='Test', countrycode="CZE", district="Testdistrict", population=300), ResultSet), True)
         
    def test_insert_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.insert, 'balderdash')
        
    def test_insert_and_get_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.insert_and_get, 'balderdash')

    def test_insert_good_arguments(self):
        #self.assertEqual(isinstance(self.db.test.insert(self.db.test.row()), list), True)
        pass

class TableUpdateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def test_update_get_data_failure(self):
        import operator
        self.assertRaises(TypeError, operator.getitem, self.db.city.where(self.db.city.id==1), 0)

    def test_update_no_arguments(self):
        self.assertRaises(DBException, self.db.city.update)

    def test_update_and_get_no_arguments(self):
        self.assertRaises(DBException, self.db.city.update_and_get)
    
    def test_update_bad_karguments(self):
        self.assertRaises(DBException, self.db.city.update, notexistingcolumn='')

    def test_update_and_get_bad_karguments(self):
        self.assertRaises(DBException, self.db.city.update_and_get, notexistingcolumn='')
         
    def test_update_good_karguments(self):
        self.assertEqual(self.db.city.where(self.db.city.id==1).update(name='Test'), None)

    def test_update_and_get_good_karguments(self):
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id==2).update_and_get(name='Test'), ResultSet), True)
         
    def test_update_bad_arguments(self):
        self.assertRaises(TypeError, self.db.city.update, 'balderdash')
        
    def test_update_and_get_bad_arguments(self):
        self.assertRaises(TypeError, self.db.city.update_and_get, 'balderdash')

class TableSelectTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db

    def test_select_get_data_failure(self):
        import operator
        self.assertRaises(TypeError, operator.getitem, self.db.city.limit(1), 0)

    def test_limit_no_arguments(self):
        self.assertRaises(TypeError, self.db.city.limit)
        
    def test_limit_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.limit, 'balderdash')
        
    def test_limit_good_arguments(self):
        self.assertEqual(isinstance(self.db.city.limit(10), TableSelect), True)
    
    def test_order_no_arguments(self):
        self.assertRaises(TypeError, self.db.city.order)
        
    def test_order_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.order, 'balderdash')    
        
    def test_order_good_arguments(self):
        self.assertEqual(isinstance(self.db.city.order(self.db.city.name), TableSelect), True)
        
    def test_order_good_arguments2(self):
        self.assertEqual(isinstance(self.db.city.order(Column('city', 'name')), TableSelect), True)    
        
    def test_select_no_arguments(self):
        self.assertEqual(isinstance(self.db.city.select(), TableSelected), True)
         
    def test_select_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.select, 'balderdash')
        
    def test_select_good_arguments(self):
        self.assertEqual(isinstance(self.db.city.select(self.db.city.name), TableSelected), True)
        
    def test_select_length(self):
        self.assertEqual(isinstance(len(self.db.city.limit(10).select()), int), True)

    def test_select_getitem(self):
        self.assertEqual(isinstance(self.db.city.limit(10).select()[0], Row), True)
        

class TableWhereTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db

    def test_where_get_data_failure(self):
        import operator
        self.assertRaises(TypeError, operator.getitem, self.db.city.where(self.db.city.id==1), 0)

    def test_where_no_arguments(self):
        self.assertRaises(DBException, self.db.city.where)
    
    def test_where_bad_karguments(self):
        self.assertRaises(DBException, self.db.city.where, balderdash='')
        
    def test_where_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.where, 'balderdash')
        
    def test_where_good_karguments(self):
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id == 1), TableWhere), True)
        
    def test_where_good_karguments2(self):
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id != 1), TableWhere), True)

    def test_where_good_karguments3(self):
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id.in_([1,2,3])), TableWhere), True)
        
class TableJoinTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def test_join_bad_argument(self):
        self.assertRaises(DBException, self.db.city.join, 'balderdash')

    def test_join_bad_on_argument(self):
        self.assertRaises(DBException, self.db.city.join, self.db.country, 'balderdash')

    def test_join_implicit_on(self):
        self.assertEqual(isinstance(self.db.city.join(self.db.country).limit(1).select()[0], Row), True)

    def test_join_explicit_on(self):
        self.assertEqual(isinstance(self.db.city.join(self.db.country, on=self.db.country.population==self.db.city.population).limit(1).select()[0], Row), True)

    def test_join_multijoin_implicit_on(self):
        self.assertEqual(isinstance(self.db.country.join(self.db.city).join(self.db.countrylanguage).limit(1).select()[0], Row), True)

    def test_join_get_data_failure(self):
        import operator
        self.assertRaises(TypeError, operator.getitem, self.db.city.join(self.db.country), 0)

class TableTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def test_column_access(self):
        self.assertEqual(isinstance(self.db.city.name, Column), True)

    def test_column_access_failure(self):
        self.assertRaises(DBException, getattr, self.db.city, 'notexistingcolumn')

    def test_where_insert(self):
        self.assertRaises(AttributeError, getattr, self.db.city.where(self.db.city.id == 1), 'insert')

    def test_limit_delete(self):
        self.assertRaises(AttributeError, getattr, self.db.city.limit(10), 'delete')

    def test_limit_update(self):
        self.assertRaises(AttributeError, getattr, self.db.city.limit(10), 'update')

    def test_select_first(self):
        self.assertEqual(isinstance(self.db.city.select().where(self.db.city.id == 1).limit(10), TableSelected), True)

    def test_select_middle(self):
        self.assertEqual(isinstance(self.db.city.limit(10).select().where(self.db.city.id == 1), TableSelected), True)

    def test_select_last(self):
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id == 1).limit(10).select(), TableSelected), True)

#class RowTest(unittest.TestCase):

    #def setUp(self):
        #self.row = db.city.insert_and_get(name='Test')[0]
        
    #def test_item_access(self):
        #self.assertEqual(type(self.row['name']), str)
        
    #def test_item_access_failure(self):
        #def get_item():
            #self.row['notexistningcolumn']
        #self.assertRaises(KeyError, get_item)
        
    #def test_update_no_arguments(self):
        #self.assertRaises(DBException, self.row.update)
        
    #def test_update_bad_arguments(self):
        #self.assertRaises(DBException, self.row.update, notexistingcolumn='')
        
    #def test_item_set_failuer(self):
        #def set_item(x):
            #self.row['notexistningcolumn'] = x
        #self.assertRaises(DBException, set_item , '')

    #def test_update_good_arguments(self):
        #self.assertEqual(type(self.row.update(name='Test')), Row)
        
    #def test_update_good_arguments2(self):
        #self.row['name'] = 'Test'
        #self.assertEqual(type(self.row.update()), Row)
        
#class RowDeleteTest(unittest.TestCase):
    
    #def setUp(self):
        #self.row = db.city.insert_and_get(name='Test')[0]
        
    #def test_delete(self):
        #self.assertEqual(self.row.delete(), None)
        
#class RowDeletedTest(unittest.TestCase):
    
    #def setUp(self):
        #self.row = db.city.insert_and_get(name='Test')[0]
        #self.row.delete()
        
    #def test_item_access_failure(self):
        #def get_item():
            #self.row['name']
        #self.assertRaises(DBException, get_item)
        
    #def test_item_set_failure(self):
        #def set_item():
            #self.row['name'] = 'Test'
        #self.assertRaises(DBException, set_item)
        
    #def test_update_failure(self):
        #self.assertRaises(DBException, self.row.update, name='Test')
        
    #def test_delete_failure(self):
        #self.assertRaises(DBException, self.row.delete)
        
def setUpModule():
    TestHelper.setUp()

def tearDownModule():
    TestHelper.tearDown()

if __name__ == '__main__':
    unittest.main()
