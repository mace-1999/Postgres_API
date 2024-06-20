'''
Application to chunk a csv and upload the data using parralelisation for quicker insert
'''
import pandas as pd
import multiprocessing
from db_connection import connect_to_db, create_table_sql_from_postgres, split_df_into_four, \
    execute_values

DB = 'devdb'
USER = 'devuser'
PASSWORD = 'changeme'
SERVER = 'localhost'
PORT = '5432'

# TODO: psycopg2.errors.DuplicateTable: relation "test" already exists

def main():
    conn = connect_to_db(DB, USER, PASSWORD, SERVER, PORT)

    # create table
    df = pd.read_csv('../MOCK_DATA.csv')
    sql_string = create_table_sql_from_postgres(df, 'MOCKDATA')

    print('sending', sql_string)
    cur = conn.cursor()

    cur.execute(sql_string)
    conn.commit()

    print(conn)
    cur.close()
    conn.close()
    # get the list of four dataframes
    df_list = split_df_into_four(df)
    # create 4 db connections.
    conn1 = connect_to_db(DB, USER, PASSWORD, SERVER, PORT)
    conn2 = connect_to_db(DB, USER, PASSWORD, SERVER, PORT)
    conn3 = connect_to_db(DB, USER, PASSWORD, SERVER, PORT)
    conn4 = connect_to_db(DB, USER, PASSWORD, SERVER, PORT)
    # create four insert statements in parallel for optimisation
    # Create our 4 processes
    if len(df_list) < 4:
        execute_values(conn1, df_list[0], 'MOCKDATA')

    else:
        p1 = multiprocessing.Process(target=execute_values, args=(DB, USER, PASSWORD, SERVER, PORT, df_list[0], 'MOCKDATA'))
        p2 = multiprocessing.Process(target=execute_values, args=(DB, USER, PASSWORD, SERVER, PORT, df_list[1], 'MOCKDATA'))
        p3 = multiprocessing.Process(target=execute_values, args=(DB, USER, PASSWORD, SERVER, PORT, df_list[2], 'MOCKDATA'))
        p4 = multiprocessing.Process(target=execute_values, args=(DB, USER, PASSWORD, SERVER, PORT, df_list[3], 'MOCKDATA'))
        # Start all the processes
        p1.start()
        p2.start()
        p3.start()
        p4.start()

        # Wait until all processes finish
        p1.join()
        p2.join()
        p3.join()
        p4.join()

    conn1.close()
    conn2.close()
    conn3.close()
    conn4.close()




if __name__ == '__main__':
    main()
