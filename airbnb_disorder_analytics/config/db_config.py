'''
db_config.py
with basic setup info for accessing PSQL;
this is mainly used along with psycopg2 where connections to PSQL can be set up using the following command:
    >> from db_config import DBInfo
    >> connection = psycopg2.connect(DBInfo.psycopg2_config)
    >> cursor = connection.cursor()
Dependencies:
    - local package: config

ruilin chen
08/09/2020
'''

from . import keys
import base64
import importlib

__all__ = ['DBInfo']

class DBInfo:
    try:
        importlib.reload(keys)
        airbnb_config = "dbname={} user='postgres' host='localhost' password={}".format('airbnb_data', base64.b64decode(keys.psql_password).decode("utf-8"))
        acs5_config = "dbname={} user='postgres' host='localhost' password={}".format('acs5', base64.b64decode(keys.psql_password).decode("utf-8"))
        crime_config = "dbname={} user='postgres' host='localhost' password={}".format('crime_data', base64.b64decode(keys.psql_password).decode("utf-8"))
    except UnicodeDecodeError:
        airbnb_config = "dbname={} user='postgres' host='localhost' password={}".format('airbnb_data', keys.psql_password)
        acs5_config = "dbname={} user='postgres' host='localhost' password={}".format('acs5', keys.psql_password)
        crime_config = "dbname={} user='postgres' host='localhost' password={}".format('crime_data', keys.psql_password)
