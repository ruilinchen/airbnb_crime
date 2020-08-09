'''
get_acs_variables_by_census_tract.py

get census tract characteristics from American Community Survey (ACS) and update them
in psql

ruilin chen
08/09/2020
'''
# todo what about census block??

# todo define a list of variables to extract from ACS

from airbnb_disorder_analytics.config.db_config import DBInfo
import psycopg2
from tqdm import tqdm

# connect to database
connection = psycopg2.connect(DBInfo.psycopg2_config)
cursor = connection.cursor()


def get_uninformed_census_tracts(num_of_tracts):
    """
    get uninformed census tracts --> census tracts whose information are yet to
    be collected from ACS

    :param num_of_tracts: int -> how many census tracts to return
    :return: list_of_census_tracts: list -> [(census_tract_id)...]
    """
    query = """SELECT census_tract_id
                    FROM census_tract
                    WHERE ?? IS NULL 
                    LIMIT {}
                    ;
                """.format(num_of_tracts)
    cursor.execute(query)
    list_of_tracts = cursor.fetchall()
    return list_of_tracts



