import unittest
from exception import DBException
from table import TableWhere, Table, TableSelect, TableSelected
from column import Column
from row import Row, ReadOnlyRow
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
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id != 1, self.db.city.name != 'Test'), TableWhere), True)

    def test_where_good_karguments3(self):
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id.in_([1,2,3]), self.db.city.name.like('Test')), TableWhere), True)

    def test_where_good_karguments4(self):
        self.assertEqual(isinstance(self.db.city.where(self.db.city.id.in_(1), self.db.city.id > 1, self.db.city.id < 10), TableWhere), True)

class TableDeleteTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db

    def test_delete(self):
        self.db.city.where(self.db.city.id == 6).delete()
        self.assertEqual(len(self.db.city.where(self.db.city.id == 6).select()), 0)
        
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

    def test_update_good_karguments2(self):
        self.assertEqual(self.db.city.where(self.db.city.id==1).update(name='Test', district='TestDistrict'), None)

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
        
    def test_order_desc_no_arguments(self):
        self.assertRaises(TypeError, self.db.city.order_desc)
        
    def test_order_desc_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.order_desc, 'balderdash')    
        
    def test_order_desc_good_arguments(self):
        self.assertEqual(isinstance(self.db.city.order_desc(self.db.city.name), TableSelect), True)
        
    def test_order_desc_good_arguments2(self):
        self.assertEqual(isinstance(self.db.city.order_desc(Column('city', 'name')), TableSelect), True)    

    def test_select_no_arguments(self):
        self.assertEqual(isinstance(self.db.city.select(), TableSelected), True)
         
    def test_select_bad_arguments(self):
        self.assertRaises(DBException, self.db.city.select, 'balderdash')
        
    def test_select_good_arguments(self):
        self.assertEqual(isinstance(self.db.city.select(self.db.city.name), TableSelected), True)

    def test_select_good_arguments2(self):
        self.assertEqual(isinstance(self.db.city.select(self.db.city.name, self.db.city.population), TableSelected), True)
        
    def test_select_length(self):
        self.assertEqual(isinstance(len(self.db.city.limit(10).select()), int), True)

    def test_select_getitem(self):
        self.assertEqual(isinstance(self.db.city.limit(10).select()[0], Row), True)
        
class TableJoinTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def test_join_bad_argument(self):
        self.assertRaises(DBException, self.db.city.join, 'balderdash')

    def test_join_bad_on_argument(self):
        self.assertRaises(DBException, self.db.city.join, self.db.country, 'balderdash')

    def test_join_bad_on_argument2(self):
        self.assertRaises(DBException, self.db.city.join, self.db.country, self.db.city.id == 1)

    def test_join_implicit_on(self):
        self.assertEqual(isinstance(self.db.city.join(self.db.country).limit(1).select()[0], ReadOnlyRow), True)

    def test_join_explicit_on(self):
        self.assertEqual(isinstance(self.db.city.join(self.db.country, on=self.db.country.population==self.db.city.population).limit(1).select()[0], ReadOnlyRow), True)

    def test_join_multijoin_implicit_on(self):
        self.assertEqual(isinstance(self.db.country.join(self.db.city).join(self.db.countrylanguage).limit(1).select()[0], ReadOnlyRow), True)

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

class RowTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def setUp(self):
        self.row = self.db.city.insert_and_get(name='RowTest', countrycode="CZE", district="Testdistrict", population=300)[0]
        
    def test_item_access(self):
        self.assertEqual(type(self.row['name']), str)
        
    def test_item_access_failure(self):
        import operator
        self.assertRaises(KeyError, operator.getitem, self.row, 'notexstingcolumn')
        
    def test_update_no_arguments(self):
        self.assertRaises(DBException, self.row.update)
        
    def test_update_bad_arguments(self):
        self.assertRaises(DBException, self.row.update, notexistingcolumn='')
        
    def test_item_set_failuer(self):
        import operator
        self.assertRaises(DBException, operator.setitem , self.row, 'notexistingcolumn', 'value')

    def test_update_good_arguments(self):
        self.assertEqual(type(self.row.update(name='New name')), Row)
        
    def test_update_good_arguments2(self):
        self.row['name'] = 'New name'
        self.assertEqual(type(self.row.update()), Row)
        
class RowDeleteTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
    
    def setUp(self):
        self.row = self.db.city.insert_and_get(name='RowTest', countrycode="CZE", district="Testdistrict", population=300)[0]
        
    def test_delete(self):
        self.assertEqual(self.row.delete(), None)
        
class RowDeletedTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
    
    def setUp(self):
        self.row = self.db.city.insert_and_get(name='RowTest', countrycode="CZE", district="Testdistrict", population=300)[0]
        self.row.delete()
        
    def test_item_access_failure(self):
        import operator
        self.assertRaises(DBException, operator.getitem, self.row, 'notexstingcolumn')
        
    def test_item_set_failure(self):
        import operator
        self.assertRaises(DBException, operator.setitem , self.row, 'notexistingcolumn', 'value')
        
    def test_update_failure(self):
        self.assertRaises(DBException, self.row.update, name='Test')
        
    def test_delete_failure(self):
        self.assertRaises(DBException, self.row.delete)

class ReadOnlyRowTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def setUp(self):
        self.row = self.db.country.join(self.db.city).join(self.db.countrylanguage).limit(1).select()[0]

    def test_item_access(self):
        self.assertEqual(type(self.row['name']), str)
        
    def test_item_access_failure(self):
        import operator
        self.assertRaises(KeyError, operator.getitem, self.row, 'notexstingcolumn')
        
    def test_no_update(self):
        self.assertRaises(AttributeError, getattr, self.row, 'update')
        
    def test_item_set_failuer(self):
        import operator
        self.assertRaises(TypeError, operator.setitem , self.row, 'firstname', 'new name')
        
    def test_no_delete(self):
        self.assertRaises(AttributeError, getattr, self.row, 'delete')

class RowRelationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def setUp(self):
        self.row = self.db.city.limit(1).select()[0]

    def test_access_country(self):
        self.assertEqual(isinstance(self.row.countrycode, Row), True)

    def test_access_languages(self):
        self.assertEqual(isinstance(self.row.countrycode.countrylanguage, ResultSet), True)

    def test_access_failure(self):
        self.assertRaises(DBException, getattr, self.row, 'notexistingrelation')

    def test_access_failure2(self):
        self.assertRaises(DBException, getattr, self.row.countrycode, 'notexistingrelation')

class SelectInTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)

    def test_select_in(self):
        for city in self.db.city.limit(10).select():
            city['name']
            city.countrycode['name']
            for language in city.countrycode.countrylanguage.select():
                language['language']
        self.assertEqual(True, True)

class NamingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(False)
        from manager import Naming
        class TestNaming(Naming):

            structure = {'city': {'pk': 'id', 'fks': ('country', 'countrycode')},
                         'country': {'pk': 'code', 'fks': ('city','capitol')},
                         'countrylanguage':{'pk': 'id', 'fks': ('country', 'countrycode')}
                            }
            def get_pk_naming(self, table):
                return self.structure[table]['pk']

            def get_fk_naming(self, table, foreign_table):
                return self.structure[table]['fks'][1]

            def match_fk_naming(self, table, attr):
                for table in self.structure.values():
                    if attr == table['fks'][1]:
                        return True
                return False

            def get_fk_column(self, table, foreign_key):
                for table in self.structure.values():
                    if foreign_key == table['fks'][1]:
                        return table['fks'][0]

        cls.db.set_naming(TestNaming())

    def setUp(self):
        self.row = self.db.city.limit(1).select()[0]

    def test_access_country(self):
        self.assertEqual(isinstance(self.row.countrycode, Row), True)

    def test_access_languages(self):
        self.assertEqual(isinstance(self.row.countrycode.countrylanguage, ResultSet), True)

class LoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_log(True)
        cls.db.set_strict(True)

    def setUp(self):
        self.db.city.limit(1).select()[0]
        self.db.country.join(self.db.city).join(self.db.countrylanguage).limit(1).select()[0]

    def test_statistics(self):
        with open('statistics.log') as f:
            import json
            record = json.loads(list(f)[-1])
            self.assertEqual(set(record['tables']), set(['country', 'countrylanguage', 'city']))

    @classmethod
    def tearDownClass(cls):
        cls.db.set_log(False)

class DebugingTest(unittest.TestCase):
        
    test_file = 'debug.test'

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        import logging
        logger = logging.getLogger("testDebug")
        logger.addHandler(logging.FileHandler(cls.test_file))
        cls.db.set_logger(logger)
        cls.db.set_strict(True)
        cls.db.set_debug(True)

    def setUp(self):
        self.db.city.limit(1).select()[0]
        self.db.country.limit(1).select()[0]

    def test_debug(self):
        with open(self.test_file, 'r') as f:
            self.assertEqual(len(f.readlines()), 2)

    @classmethod
    def tearDownClass(cls):
        import os
        os.remove(cls.test_file)
        cls.db.set_debug(False)
        cls.db.set_logger(None)

class MaterializedViewTest(unittest.TestCase):

    view_name = 'test_view'

    @classmethod
    def setUpClass(cls):
        cls.db = TestHelper.db
        cls.db.set_strict(True)
        cls.db.create_mview(cls.view_name, cls.db.country.join(cls.db.city).join(cls.db.countrylanguage).select())

    def get_view(self):
        return getattr(self.db, self.view_name)

    def test_access(self):
        self.assertEqual(isinstance(self.get_view().select().limit(1)[0], Row), True)

    def test_city_delete(self):
        self.db.city.where(self.db.city.id == 2).delete()
        self.assertEqual(len(self.get_view().where(self.get_view().city_id == 2).select()), 0)
        
    def test_language_insert(self):
        row = self.db.countrylanguage.insert_and_get(language='Ostravstina', countrycode="CZE", isofficial=False, percentage=10)[0]
        self.assertEqual(isinstance(self.get_view().where(self.get_view().countrylanguage_id == row['id']).select()[0], Row), True)

    def test_city_update(self):
        self.db.city.where(self.db.city.id == 33).update(name='Test')
        self.assertEqual(self.get_view().where(self.get_view().city_id == 33).select()[0]['city_name'], 'Test')

    @classmethod
    def tearDownClass(cls):
        cls.db.drop_mview(cls.view_name)

def setUpModule():
    TestHelper.setUp()

def tearDownModule():
    TestHelper.tearDown()

if __name__ == '__main__':
    unittest.main()
