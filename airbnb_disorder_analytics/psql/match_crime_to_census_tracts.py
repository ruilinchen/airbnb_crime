'''
match_crime_to_census_tracts.py

this script includes functions that match crime incidents to their corresponding neighborhoods defined
as the census tracts. it is also defined to automatically save all the matching results to database by
updating the collection "census_tract" and "census_block" in database "crime_data" and adding
new columns called "census_tract_id" and "census_block_id" to each of the items in collection "crime_incident".

matching is based on the longitude and latitude of the listing.

Dependencies:
    - third-party packages: psycopg2, censusgeocode
    - local packages: config.db_config

ruilin chen
08/15/2020
'''
# system import
import os
import time
import random
from tqdm import tqdm
import pandas as pd
# third-party import
import psycopg2
import censusgeocode as cg
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo


class CrimeDB:
    def __init__(self):
        self.acs5_cursor = None
        self.acs5_connection = None
        self.crime_connection = None
        self.crime_cursor = None
        self.batch_size = 5000
        self.city_to_crime_filename = {}
        self.data_folder = '/home/rchen/Documents/github/airbnb_crime/data/crime_data'
        self.crime_filename_by_city = {
            'austin': 'Austin_Crime_Reports.csv',
            'boston': 'BOS_crime_incident_from_2015.csv',
            'chicago': 'Chicago_Crimes_-_2019.csv',
            'la': 'LA_Crime_Data_from_2010_to_2019.csv',
            'nyc': 'NY_Complaint_Data_2019.csv',
            'seattle': 'Seattle_from_2008_to_2019.csv',
            'sf': ['SF_Police_Department_Incident_Reports__2018_to_Present.csv',
                    'SF_Police_Department_Incident_Reports__Historical_2003_to_May_2018.csv']
        }  # not complete; some cities have more than one file
        self.crime_columns_by_city = {
            'austin': {'incident_id': 'Incident Number',
                       'description': 'Highest Offense Description',
                       'longitude': 'Longitude',
                       'latitude': 'Latitude',
                       'date': 'Occurred Date Time',
                       'address': 'Address'},
            'boston': {'incident_id': 'INCIDENT_NUMBER',
                       'description': 'OFFENSE_DESCRIPTION',
                       'longitude': 'Long',
                       'latitude': 'Lat',
                       'date': 'OCCURRED_ON_DATE',
                       },
            'chicago': {'incident_id': 'ID',
                       'description': 'Description',
                       'longitude': 'Longitude',
                       'latitude': 'Latitude',
                       'date': 'Date'},
            'la': {'incident_id': 'DR_NO',
                       'description': 'Crm Cd Desc',
                       'longitude': 'LON',
                       'latitude': 'LAT',
                       'date': 'DATE OCC'},
            'nyc':{'incident_id': 'CMPLNT_NUM',
                       'description': 'OFNS_DESC',
                       'longitude': 'Longitude',
                       'latitude': 'Latitude',
                       'date': ['CMPLNT_FR_DT', 'CMPLNT_FR_TM']},
            'seattle':{'incident_id': 'ID',
                       'description': 'Description',
                       'longitude': 'Longitude',
                       'latitude': 'Latitude',
                       'date': 'Date'},
            'sf':[{'incident_id': 'Incident ID',
                       'description': 'Incident Description',
                       'longitude': 'Longitude',
                       'latitude': 'Latitude',
                       'date': 'Incident Datetime'},
                  {'incident_id': 'IncidntNum',
                   'description': 'Descript',
                   'longitude': 'X',
                   'latitude': 'Y',
                   'date': ['Date','Time']}]
        }
        self.city_to_state = {
            'austin': 'TX',
            'boston': 'MA',
            'chicago': 'IL',
            'la': 'CA',
            'nyc': 'NY',
            'seattle': 'WA',
            'sf': 'CA'
        }


    def connect_to_db(self, acs5={'connection': '', 'cursor': ''},
                      crime={'connection': '', 'cursor': ''}):
        self.acs5_cursor = acs5['cursor']
        self.acs5_connection = acs5['connection']
        self.crime_cursor = crime['cursor']
        self.crime_connection = crime['connection']

    def copy_census_tracts_from_acs5(self):
        acs5_query = """SELECT census_tract_id, census_tract_code, state_id, county_id, county_name, state_abbr, state
                            FROM census_tracts
                            ;
                        """
        self.acs5_cursor.execute(acs5_query)
        results = self.acs5_cursor.fetchall()

        crime_query = """INSERT INTO census_tracts (census_tract_id, census_tract_code, state_id, county_id, county_name, state_abbr, state) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (census_tract_id) DO NOTHING
                            RETURNING census_tract_id
                            ;
                        """
        for index, result in tqdm(enumerate(results), total=len(results)):
            self.crime_cursor.execute(crime_query, result)
            if index % self.batch_size == 0:
                self.crime_connection.commit()

    def drop_crimes_by_year(self):
        query = """DELETE FROM crime_incident
                    WHERE year < 2014
                    RETURNING *
                    ;
                    """
        self.crime_cursor.execute(query)
        count_of_results = len(self.crime_cursor.fetchall())
        self.crime_connection.commit()
        print('deleted records:', count_of_results)

    def _insert_by_pandas(self, city, crime_df, crime_columns=None):
        if crime_columns is None:
            crime_columns = self.crime_columns_by_city[city]
        error_count = 0
        if isinstance(crime_columns['date'], list):
            crime_df['combined_date_time'] = crime_df[crime_columns['date'][0]] + ' ' + crime_df[
                crime_columns['date'][1]]
            crime_columns['date'] = 'combined_date_time'
        for index, row in tqdm(crime_df.iterrows(), total=len(crime_df)):
            incident_id = row[crime_columns['incident_id']]
            description = row[crime_columns['description']]
            longitude = row[crime_columns['longitude']]
            latitude = row[crime_columns['latitude']]
            try:
                year = pd.to_datetime(row[crime_columns['date']]).year
            except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                continue
            state = self.city_to_state[city]
            date = row[crime_columns['date']]
            if pd.isnull(date):
                print(row)
                error_count += 1
            else:
                if pd.isnull(longitude) and pd.isnull(latitude) and 'address' in crime_columns:
                    address = row[crime_columns['address']]
                    query = """INSERT INTO crime_incident (incident_id, description, address, year, city, state, date) 
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (incident_id) DO NOTHING
                                            RETURNING incident_id
                                            ;
                                        """
                    values = (incident_id, description, address, year, city, state, date)
                    self.crime_cursor.execute(query, values)
                    if index % self.batch_size == 0:
                        self.crime_connection.commit()
                elif pd.isnull(longitude) and pd.isnull(latitude):
                    error_count += 1
                else:
                    query = """INSERT INTO crime_incident (incident_id, description, longitude, latitude, year, city, state, date) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (incident_id) DO NOTHING
                                        RETURNING incident_id
                                        ;
                                    """
                    values = (incident_id, description, longitude, latitude, year, city, state, date)
                    self.crime_cursor.execute(query, values)
                    if index % self.batch_size == 0:
                        self.crime_connection.commit()
        print('== error count:', error_count)

    def insert_crimes_into_psql(self, city):
        if isinstance(self.crime_filename_by_city[city], list):
            for index, filename in enumerate(self.crime_filename_by_city[city]):
                crime_df = pd.read_csv(os.path.join(self.data_folder, filename))
                self._insert_by_pandas(city, crime_df,  crime_columns=self.crime_columns_by_city[city][index])
        else:
            crime_df = pd.read_csv(os.path.join(self.data_folder, self.crime_filename_by_city[city]))
            self._insert_by_pandas(city, crime_df)


if __name__ == '__main__':
    # connect to database
    crime_connection = psycopg2.connect(DBInfo.crime_config)
    crime_cursor = crime_connection.cursor()
    acs5_connection = psycopg2.connect(DBInfo.acs5_config)
    acs5_cursor = acs5_connection.cursor()
    # set up CrimeDB
    cdb = CrimeDB()
    cdb.connect_to_db(acs5={'connection': acs5_connection, 'cursor':acs5_cursor},
                      crime={'connection': crime_connection, 'cursor': crime_cursor})
    # copy the table census tracts from acs5
    # cdb.copy_census_tracts_from_acs5()
    # import crime data from pandas to psql
    cdb.insert_crimes_into_psql('sf')
    # cdb.drop_crimes_by_year()

    # todo geolocate using the available information??
    # todo test the method first on DC -> census tract already known
