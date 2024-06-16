import unittest

import pandas as pd

from app.db_connection import connect_to_db, open_and_return_csv
from psycopg2 import OperationalError
from unittest.mock import patch


class TestConnector(unittest.TestCase):
    @patch('app.db_connection.psycopg2')
    def test_db_connection_success(self, patched_mock):
        '''
         Check when successful the connection is returned.
        '''
        patched_mock.connect.return_value = 1
        self.assertEqual(connect_to_db('test', 'test', 'pass123', 'testhot', '1111'), 1)

    @patch('app.db_connection.psycopg2')
    def test_db_connection_failed(self, patched_mock):
        '''
         Check false is returned if db resulted in operational error 7 times.
        '''

        patched_mock.connect.side_effect = [OperationalError] * 7
        self.assertEqual(connect_to_db('test', 'test', 'pass123', 'testhot', '1111'), False)

    @patch('app.db_connection.psycopg2')
    def test_db_connection_failed_10_times(self, patched_mock):
        '''
         Check false is returned if db resulted in operational error 7 or more times.
        '''

        patched_mock.connect.side_effect = [OperationalError] * 10
        self.assertEqual(connect_to_db('test', 'test', 'pass123', 'testhot', '1111'), False)

    @patch('app.db_connection.pd')
    def test_csv_return(self, patched_mock):
        '''
        Test opening csv returns correctly
        '''
        test_df = {'One': [1,2,3], 'Two' : [4,5,6]}
        patched_mock.read_csv.return_value = pd.DataFrame(test_df)
        df = open_and_return_csv('random_test')
        self.assertEqual(len(df), 3)


    def test_creating_table_sql(self):
        '''
        Test the compiling of the create table sql string
        '''



if __name__ == '__main__':
    unittest.main()