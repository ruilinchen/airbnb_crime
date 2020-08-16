'''
match_crime_to_census_tracts.py

this script includes functions that match crime incidents to their corresponding neighborhoods defined
as the census tracts. it is also defined to automatically save all the matching results to database by
updating the collection "crime_incident" in database "crime_data".

matching is based on the longitude and latitude of the listing.

Dependencies:
    - third-party packages: psycopg2, censusgeocode
    - local packages: config.db_config, psql.match_property_to_census_tracts

ruilin chen
08/15/2020
'''
# system import
import os
import sys
from tqdm import tqdm
import pandas as pd
# third-party import
import psycopg2
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.psql.match_property_to_census_tracts import get_census_tract_by_geo_info

class CrimeDB:
    """
    first step: insert crime incidents into psql
    second step: use censusgeocode to geolocate some crime incidents
    # todo third step: rely on the geolocated incidents to infer other incidents' census tracts
                -> by passing the censusgeocode API which is slow
    """
    def __init__(self):
        self.acs5_cursor = None
        self.acs5_connection = None
        self.crime_connection = None
        self.crime_cursor = None
        self.airbnb_connection = None
        self.airbnb_cursor = None
        self.batch_size = 200
        self.city_to_crime_filename = {}
        self.data_folder = '/home/rchen/Documents/github/airbnb_crime/data/crime_data'
        self.crime_filename_by_city = {
            'austin': 'Austin_Crime_Reports.csv',
            'boston': 'BOS_crime_incident_from_2015.csv',
            'chicago': 'Chicago_Crimes_-_2019.csv',
            'dc': 'DC_Crime_Incidents_in_2019.csv',
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
                   'date': ['Date','Time']}],
            'dc': {'incident_id': 'CCN',
                   'description': 'OFFENSE',
                   'longitude': 'LONGITUDE',
                   'latitude': 'LATITUDE',
                   'date': 'REPORT_DAT',
                   'census_tract': 'CENSUS_TRACT'
                   }
        }
        self.city_to_state = {
            'austin': 'TX',
            'boston': 'MA',
            'chicago': 'IL',
            'dc': 'District of Columbia',
            'la': 'CA',
            'nyc': 'NY',
            'seattle': 'WA',
            'sf': 'CA'
        }

    def connect_to_db(self, acs5={'connection': '', 'cursor': ''},
                      crime={'connection': '', 'cursor': ''},
                      airbnb={'connection': '', 'cursor': ''}):
        self.acs5_cursor = acs5['cursor']
        self.acs5_connection = acs5['connection']
        self.crime_cursor = crime['cursor']
        self.crime_connection = crime['connection']
        self.airbnb_cursor = airbnb['cursor']
        self.airbnb_connection = airbnb['connection']

    def copy_census_tracts_from_acs5(self):
        """
        copies the entire census_tracts table from acs5 into crime_data
        """
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

    def drop_crimes_by_year(self, year):
        """
        keep only crimes in recent years in the database
        :param year: int
        :return:
        """
        query = """DELETE FROM crime_incident
                    WHERE year < %s
                    RETURNING *
                    ;
                    """
        self.crime_cursor.execute(query, (year, ))
        count_of_results = len(self.crime_cursor.fetchall())
        self.crime_connection.commit()
        print('deleted records:', count_of_results)

    def _insert_by_pandas(self, city, crime_df, crime_columns=None):
        """
        insert crime incidents stored in a pandas dataframe into psql

        :param city: str
        :param crime_df: a pandas dataframe with the raw data
        :param crime_columns: a dictionary with the keys being psql columns and values being the
                                corresponding pandas columns
        :return: None
        """
        if crime_columns is None:  # if the user doesn't specify crime_columns, look for them in class attributes
            crime_columns = self.crime_columns_by_city[city]
        error_count = 0
        if isinstance(crime_columns['date'], list):
            # if the crime date are stored in two separate columns, one for date and one for time of day,
            # create a new column and combine them
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
            # if the provided date is out of bound, ignore the entry
            except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                error_count += 1
                continue
            state = self.city_to_state[city]
            date = row[crime_columns['date']]
            # if date is not provided, print the row,
            # ignore the entry and increment the error_count by 1
            if pd.isnull(date):
                print(row)
                error_count += 1
            else:
                if pd.isnull(longitude) and pd.isnull(latitude) and 'address' in crime_columns:
                    # if raw data doesn't have long/lat info, insert the address into psql so that
                    # later census tracts can be geolocated by using the address
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
                    # if none of the longitude, latitude and address columns have meaningful information,
                    # ignore the record and increment the error_count by 1
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
        """
        insert crime incidents into psql by city

        :param city: str
        :return: None
        """
        if isinstance(self.crime_filename_by_city[city], list):
            # if raw data for the city are stored in more than one file
            for index, filename in enumerate(self.crime_filename_by_city[city]):
                crime_df = pd.read_csv(os.path.join(self.data_folder, filename))
                self._insert_by_pandas(city, crime_df,  crime_columns=self.crime_columns_by_city[city][index])
        else:
            crime_df = pd.read_csv(os.path.join(self.data_folder, self.crime_filename_by_city[city]))
            self._insert_by_pandas(city, crime_df)

    def local_geolocating(self, city, year, verbose=True):
        """
        geolocate crime incidents using the censusgeocode API and update the
        results into psql

        :param city: str -> process crime incidents that happen in one city at a time
        :param year: int -> process crime incidents that happen in one year at a time
        :param verbose: boolean -> whether to print outputs as the program runs
        :return:
        """
        query = """SELECT incident_id, longitude, latitude
                    FROM crime_incident
                    WHERE longitude IS NOT NULL
                    AND longitude != 'NaN'
                    AND census_tract_id IS NULL
                    AND year = %s
                    LIMIT {}
                    ;
                    """.format(self.batch_size)
        self.crime_cursor.execute(query, (year, ))
        results = self.crime_cursor.fetchall()
        if len(results): # check if there are still records waiting to be geolocated
            for result in tqdm(results, total=len(results)):
                incident_id = result[0]
                longitude = result[1]
                latitude = result[2]
                output = get_census_tract_by_geo_info(longitude, latitude, verbose)
                cdb._update_census_block_to_psql(incident_id, output['census_block_id'], output['census_tract_id'],
                                                 verbose)
                self.crime_connection.commit()
        else:
            print('all the crime incidents are already labelled')
            sys.exit()

    def _update_census_block_to_psql(self, incident_id, census_block_id, census_tract_id, verbose=True):
        """
        insert the matching result between crime incidents and their census block info into psql
        as census block is defined to be child of census tract, the update also requires information
        on the census tract to which this block belongs.

        called inside the local_geolocating function

        :param incident_id: str
        :param census_block_id: str
        :param census_tract_id: str
        :param: verbose: boolean -> whether to print detailed outputs as the program runs
        :return: True
        """
        query = """UPDATE crime_incident
                    SET census_block_id = %s,
                        census_tract_id = %s
                    WHERE incident_id = %s 
                    RETURNING incident_id
                    """
        self.crime_cursor.execute(query, (census_block_id, census_tract_id, incident_id))
        query_output = self.crime_cursor.fetchone()
        if verbose:
            print('updated property:', query_output)
        return True


if __name__ == '__main__':
    # connect to database
    crime_connection = psycopg2.connect(DBInfo.crime_config)
    crime_cursor = crime_connection.cursor()
    acs5_connection = psycopg2.connect(DBInfo.acs5_config)
    acs5_cursor = acs5_connection.cursor()
    airbnb_connection = psycopg2.connect(DBInfo.airbnb_config)
    airbnb_cursor = airbnb_connection.cursor()
    # set up CrimeDB
    cdb = CrimeDB()
    cdb.connect_to_db(acs5={'connection': acs5_connection, 'cursor':acs5_cursor},
                      crime={'connection': crime_connection, 'cursor': crime_cursor},
                      airbnb={'connection': airbnb_connection, 'cursor': airbnb_cursor})
    # copy the table census tracts from acs5
    # cdb.copy_census_tracts_from_acs5()
    # import crime data from pandas to psql
    # cdb.insert_crimes_into_psql('sf')
    #cdb.drop_crimes_by_year()
    while True:
        cdb.local_geolocating(city='boston', year=2019, verbose=False)

