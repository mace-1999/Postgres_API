'''
Application to chunk a csv and upload the data using parralelisation for quicker insert
'''

from db_connection import connect_to_db

conn = connect_to_db('devdb', 'devuser', 'changeme', 'localhost', '5432')

print(conn)

conn.close()