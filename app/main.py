'''
Application to chunk a csv and upload the data using parralelisation for quicker insert
'''
import pandas as pd

from db_connection import connect_to_db, create_table_sql_from_postgres


def main():
    conn = connect_to_db('devdb', 'devuser', 'changeme', 'localhost', '5432')

    df = pd.read_csv('../test.csv')
    sql_string = create_table_sql_from_postgres(df, 'test')

    print('sending', sql_string)
    cur = conn.cursor()

    cur.execute(sql_string)
    conn.commit()

    print(conn)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
