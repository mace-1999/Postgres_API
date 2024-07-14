import json

'''
Collect configuration data from the config.json file. 
'''

with open('config.json') as json_file:
    DATA = json.load(json_file)


# Load in the variables
DB = DATA['DB']
USER = DATA['USER']
PASSWORD = DATA['PASSWORD']
SERVER = DATA['SERVER']
PORT = DATA['PORT']
FILEPATH = DATA['FILEPATH']
TABLE_NAME = DATA['TABLE_NAME']