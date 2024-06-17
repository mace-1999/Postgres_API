import psycopg2
from psycopg2 import OperationalError
import pandas as pd


def connect_to_db(dbname, user, password, host, port) -> psycopg2.connect:
    '''
    attempt 5 db connections otherwise fail.
    :param dbname:
    :param user:
    :param password:
    :return: conn
    '''
    attempts = 0
    while attempts <= 6:
        try:
            conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            return conn

        except OperationalError:
            attempts += 1

    return False


def open_and_return_csv(csv_name: str) -> pd.DataFrame:
    '''
    Open and return a csv
    :return: pandas dataframe
    '''
    return pd.read_csv(csv_name)


def create_table_sql_from_postgres(dataframe: pd.DataFrame, name: str) -> str:
    '''
    Generate the create sql as a string for a specific csv.
    Start as varchar.
    '''
    cols = list(dataframe.columns)

    sql_string = f'CREATE TABLE {name} ( '
    for idx, i in enumerate(cols):
        if (idx + 1) != len(cols):
            sql_string += f'{i} varchar,'
        else:
            sql_string += f'{i} varchar'

    sql_string += ');'

    return sql_string

