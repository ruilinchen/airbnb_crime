"""
link_airbnb_with_acs.py

this program get the ACS5 characteristics of the neighborhood to which each of the airbnb listings belongs

includes test case for:
    - the Airbnb2ACS class

ruilin
08/11/2020
"""

# system import
import os
import sys
import pandas as pd
from tqdm import tqdm
# third-party import
import psycopg2
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo

__all__ = ['Airbnb2ACS']

class Airbnb2ACS:
    """
    get the ACS5 characteristics of the neighborhood to which each of the airbnb listings belongs
    """
    def __init__(self):
        # psql connection to the airbnb database
        self.airbnb_connection = psycopg2.connect(DBInfo.airbnb_config)
        self.airbnb_cursor = self.airbnb_connection.cursor()
        # psql connection to the acs5 database
        self.acs_connection = psycopg2.connect(DBInfo.acs_config)
        self.acs_cursor = self.acs_connection.cursor()

    def get_one_airbnb_listing(self):
        """

        :return: property_id, and geo-info of a randomly chosen airbnb listing
        """
        query = """SELECT property_id, census_block_id, census_tract_id 
                    FROM property 
                    WHERE property.census_block_id IS NOT NULL
                    LIMIT 1
                    ;
                    """
        self.airbnb_cursor.execute(query)
        result = self.airbnb_cursor.fetchone()
        print('one airbnb listing:', result)
        return result

    def get_acs5_features_by_census_tract(self, census_tract_id):
        """
        :param census_tract_id : 11-digit str
        :return: values of ACS5 variables for this census tract
        """
        query = """SELECT variable_id, estimate, margin_of_error
                    FROM variable_by_tract
                    WHERE census_tract_id = %s
                    ;
                    """
        self.acs_cursor.execute(query, (census_tract_id,))
        results = self.acs_cursor.fetchall()
        print(results)
        return results

    def get_acs5_features_by_census_block(self, census_block_group_id):
        """

        :param census_block_group_id: 12-digit str
        :return: values of ACS5 variables for this census block group
        """
        census_tract_id = census_block_group_id[:11]
        census_block_group = census_block_group_id[11]
        query = """SELECT variable_id, estimate, margin_of_error
                    FROM variable_by_block
                    WHERE census_tract_id = %s
                    AND census_block_group = %s
                    ;
                    """
        self.acs_cursor.execute(query, (census_tract_id, census_block_group))
        results = self.acs_cursor.fetchall()
        print(results)

if __name__ == '__main__':
    a2a = Airbnb2ACS()
    property_id, census_block_id, census_tract_id = a2a.get_one_airbnb_listing()
    print('airbnb_by_census_tract:', property_id)
    a2a.get_acs5_features_by_census_tract(census_tract_id)
    print('airbnb_by_census_block:', property_id)
    a2a.get_acs5_features_by_census_block(census_block_id)

