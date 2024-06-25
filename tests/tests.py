import unittest

import pandas as pd
import psycopg2

import app.db_connection
from app.db_connection import connect_to_db, open_and_return_csv, \
    create_table_sql_from_postgres, split_df_into_four, execute_values, get_number_records
from psycopg2 import OperationalError, DatabaseError, connect
from unittest.mock import patch, MagicMock


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
        test_df = {'One': [1, 2, 3], 'Two': [4, 5, 6]}
        patched_mock.read_csv.return_value = pd.DataFrame(test_df)
        df = open_and_return_csv('random_test')
        self.assertEqual(len(df), 3)

    def test_creating_table_sql(self):
        '''
        Test the compiling of the create table sql string
        '''
        test_df = {'One': [1, 2, 3], 'Two': [4, 5, 6]}
        df = pd.DataFrame(test_df)
        sql_string = create_table_sql_from_postgres(df, 'testdf')

        self.assertEqual(sql_string, 'CREATE TABLE testdf ( One varchar,Two varchar);')

    def test_split_db_into_four(self):
        '''
        Confirm function returns four equal dataframes if multiple of four
        '''
        test_df = {'One': [1, 2, 3, 4], 'Two': [4, 5, 6, 7]}
        df = pd.DataFrame(test_df)
        dfs_returned = split_df_into_four(df)

        self.assertEqual(len(dfs_returned), 4)
        self.assertEqual(len(dfs_returned[0]), 1)
        self.assertEqual(len(dfs_returned[1]), 1)
        self.assertEqual(len(dfs_returned[2]), 1)
        self.assertEqual(len(dfs_returned[3]), 1)

    def test_last_df_makes_up_to_four(self):
        '''
        If df not multiple of four - fourth dataframe shall be the remainder.
        '''
        test_df = {'One': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'Two': [4, 5, 6, 7, 1, 1, 1, 2, 3, 4]}
        df = pd.DataFrame(test_df)
        dfs_returned = split_df_into_four(df)
        self.assertEqual(len(dfs_returned), 4)
        self.assertEqual(len(dfs_returned[0]), 3)
        self.assertEqual(len(dfs_returned[1]), 3)
        self.assertEqual(len(dfs_returned[2]), 3)
        self.assertEqual(len(dfs_returned[3]), 1)

    def test_df_below_four(self):
        '''
        If df is below four then return one list of df with the rest of the vals being False.
        '''
        test_df = {'One': [1, 2, 4], 'Two': [4, 2, 2]}
        df = pd.DataFrame(test_df)
        returned_list = split_df_into_four(df)
        self.assertEqual(returned_list, [df])

    @patch('app.db_connection.connect_to_db')
    @patch('app.db_connection.extras')
    def test_postgres_insert_error(self, mock_extras, mock_connect):
        '''
        Test postgres insert statement
        '''

        mock_extras.execute_values.return_value = 'None'
        mock_connect.return_value = MagicMock()
        mock_connect.return_value.commit.side_effect = DatabaseError('Mock Error!')

        res = execute_values('dbname', 'user', 'password', 'host', 'port',
                             pd.DataFrame({'One': [1, 2, 3, 4], 'Two': [4, 5, 6, 7]}), 'test')
        self.assertEqual(res, False)

    @patch('app.db_connection.connect_to_db')
    def test_calculate_table_count(self, mock_connect):
        '''
        Test calculating rows in a postgres table
        '''
        mock_connect.return_value = MagicMock()
        mock_connect.return_value.cursor.return_value.fetchone.return_value = (1000,)

        self.assertEqual(get_number_records('dbname', 'user', 'password', 'host', 'port','test'),
                         1000)



if __name__ == '__main__':
    unittest.main()
