import psycopg2
from psycopg2 import OperationalError, extras
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP


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


def split_df_into_four(df: pd.DataFrame) -> list:
    '''
    Split the df into four equal lengths - last length shall be however many to get to four.
    '''

    # return input as list if df is less than four.
    if len(df) < 4:
        return [df]

    length_of_df_divide_four = len(df) / 4

    # incr_length is length of df divide rounded of 4.
    incr_length = int(Decimal(length_of_df_divide_four).to_integral_value(rounding=ROUND_HALF_UP))
    list_of_dfs = []
    offset = 0
    # loop three times
    for i in range(0, 3):
        list_of_dfs.append(df[offset: offset + incr_length])
        offset += incr_length

    list_of_dfs.append(df[offset:])

    return list_of_dfs

# https://www.geeksforgeeks.org/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/
def execute_values(dbname, user, password, host, port, df, table):

    conn = connect_to_db(dbname, user, password, host, port)
    print(conn)
    tuples = [tuple(row) for row in df.itertuples(index=False)]
    print(tuples)
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()

    except psycopg2.DatabaseError as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return False
    print("the dataframe is inserted")
    cursor.close()
    return True

