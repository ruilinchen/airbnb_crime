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
import numpy as np
import pickle
import random
import time
from pprint import pprint
# third-party import
import psycopg2
from scipy.spatial import cKDTree
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score
import censusgeocode as cg
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.psql.match_property_to_census_tracts import get_census_tract_by_geo_info
from airbnb_disorder_analytics.config.us_states import USStates



class NearestClassifier:
    def __init__(self, X, Y, k):
        self.random_state = 123
        self.all_X = np.array(X).reshape(-1, k)
        self.all_Y = np.array(Y)
        self.train_X = None
        self.train_Y = None
        self.valid_X = None
        self.valid_Y = None
        self.split_ratio = 0.75
        self.clf = LogisticRegression(random_state=self.random_state)

    def train_classifier(self):
        self.train_X, self.valid_X, self.train_Y, self.valid_Y = train_test_split(
            self.all_X, self.all_Y, test_size=self.split_ratio, random_state=self.random_state)
        self.clf.fit(X=self.train_X, y=self.train_Y)
        predicted_Y = self.clf.predict(self.valid_X)
        precision_rate = precision_score(self.valid_Y, predicted_Y)
        print('precision rate:', precision_rate)
        return precision_rate

    def save_classifier(self, pkl_name):
        with open(pkl_name, 'wb') as file:
            pickle.dump(self.clf, file)
        print('saved model to', pkl_name)

    def load_classifer(self, pkl_name):
        with open(pkl_name, 'rb') as file:
            pickle_model = pickle.load(file)
        return pickle_model

class CrimeDB:
    """
    first step: insert crime incidents into psql
    second step: use censusgeocode to geolocate some crime incidents
    # todo third step: rely on the geolocated incidents to infer other incidents' census tracts
                -> by passing the censusgeocode API which is slow
    """
    def __init__(self, state_abbr):
        self.acs5_cursor = None
        self.acs5_connection = None
        self.crime_connection = None
        self.crime_cursor = None
        self.airbnb_connection = None
        self.airbnb_cursor = None
        self.uss = USStates()
        self.batch_size = 200
        self.state_abbr = state_abbr
        self.state_id = self.uss.abbr_to_fips[state_abbr]
        self.city = None
        self.all_geolocated_points = None
        self.all_geolocated_nodes = None
        self.matching_classifier_name = f'matching_classifier_{self.state_abbr.upper()}.pkl'
        self.city_to_crime_filename = {}
        self.data_folder = '/home/rchen/Documents/github/airbnb_crime/data/crime_data'
        self.crime_filename_by_city = {
            'austin': 'Austin_Crime_Reports.csv',
            'boston': 'BOS_crime_incident_from_2015.csv',
            'chicago': 'Chicago_Crimes_-_2019.csv',
            'dc': 'DC_Crime_Incidents_in_2019.csv',
            'la': 'LA_Crime_Data_from_2010_to_2019.csv',
            'nyc': ['NY_Complaint_Data_2020.csv', 'NY_Complaint_Data_from_2014_to_2019.csv'],
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
            'nyc':[{'incident_id': 'CMPLNT_NUM',
                       'description': 'OFNS_DESC',
                       'longitude': 'Longitude',
                       'latitude': 'Latitude',
                       'date': ['CMPLNT_FR_DT', 'CMPLNT_FR_TM']},
                   {'incident_id': 'CMPLNT_NUM',
                    'description': 'OFNS_DESC',
                    'longitude': 'Longitude',
                    'latitude': 'Latitude',
                    'date': ['CMPLNT_FR_DT', 'CMPLNT_FR_TM']}],
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
        self.cwd = '/home/rchen/Documents/github/airbnb_crime/airbnb_disorder_analytics/psql'
        self.clf = self.load_classifer(os.path.join(self.cwd, self.matching_classifier_name))

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

    def _insert_by_pandas(self, city, crime_df, crime_columns=None, year_threshold=None):
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
                if year_threshold is not None and year < year_threshold:
                    continue
            # if the provided date is out of bound, ignore the entry
            except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                error_count += 1
                continue
            state_abbr = self.city_to_state[city]
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
                    values = (incident_id, description, address, year, city, state_abbr, date)
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
                    values = (incident_id, description, longitude, latitude, year, city, state_abbr, date)
                    self.crime_cursor.execute(query, values)
                    if index % self.batch_size == 0:
                        self.crime_connection.commit()
        self.crime_connection.commit()
        print('== error count:', error_count)

    def insert_crimes_into_psql(self, city, year_threshold=None):
        """
        insert crime incidents into psql by city

        :param city: str
        :return: None
        """
        if isinstance(self.crime_filename_by_city[city], list):
            # if raw data for the city are stored in more than one file
            for index, filename in enumerate(self.crime_filename_by_city[city]):
                crime_df = pd.read_csv(os.path.join(self.data_folder, filename))
                self._insert_by_pandas(city, crime_df,  crime_columns=self.crime_columns_by_city[city][index],
                                       year_threshold=year_threshold)
        else:
            crime_df = pd.read_csv(os.path.join(self.data_folder, self.crime_filename_by_city[city]))
            self._insert_by_pandas(city, crime_df)

    def get_geolocated_points_by_state(self):  # include geolocated records in both airbnb_data and crime_data
        if self.all_geolocated_points is None:
            query = """SELECT crime_incident.longitude, crime_incident.latitude, crime_incident.census_tract_id
                        FROM crime_incident, census_tracts
                        WHERE crime_incident.census_tract_id = census_tracts.census_tract_id
                        AND crime_incident.longitude IS NOT NULL
                        AND crime_incident.longitude != 'NaN'
                        AND crime_incident.census_tract_id IS NOT NULL
                        AND crime_incident.census_tract_id != 'NaN'
                        AND census_tracts.state_id = %s
                        ;
                        """
            crime_cursor.execute(query, (self.state_id,))
            state = self.uss.abbr_to_name[self.state_abbr]
            list_of_records = crime_cursor.fetchall()
            if len(list_of_records) > 200000:
                random.shuffle(list_of_records)
                list_of_records = list_of_records[:200000]
            query = """SELECT property.longitude, property.latitude, property.census_tract_id
                        FROM property, census_tract
                        WHERE property.census_tract_id = census_tract.census_tract_id
                        AND property.longitude IS NOT NULL
                        AND property.longitude != 'NaN'
                        AND property.census_tract_id IS NOT NULL
                        AND property.census_tract_id != 'NaN'
                        AND property.state = %s
                        ;
                    """
            airbnb_cursor.execute(query, (state,))
            another_list_of_records = airbnb_cursor.fetchall()
            random.shuffle(another_list_of_records)
            self.all_geolocated_points = list_of_records + another_list_of_records
            random.shuffle(self.all_geolocated_points)
            self.all_geolocated_points = self.all_geolocated_points[:200000]
            self.all_geolocated_nodes = np.array([[point[0], point[1]] for point in self.all_geolocated_points])

    @staticmethod
    def ckdnearest(all_nodes, target_node, k=1):
        # k specifies the number of smallest values to return
        btree = cKDTree(target_node)
        dist, idx = btree.query(all_nodes, k=1)
        dist = dist.flatten()
        if k > 1:
            indices_of_min = np.argpartition(dist, k)[:k]
            return indices_of_min, dist[indices_of_min]
        else:
            return np.argmin(dist), min(dist)

    @staticmethod
    def load_classifer(pkl_name):
        with open(pkl_name, 'rb') as file:
            pickle_model = pickle.load(file)
        return pickle_model

    def predict_matching(self, input):
        output = self.clf.predict(np.array([input]).reshape(1, -1))
        return bool(output[0])

    def geolocate_point(self, longitude, latitude, verbose):
        target_node = np.array([[longitude, latitude]])
        nearest_node_index, nearest_dist = self.ckdnearest(self.all_geolocated_nodes, target_node)
        matched_flag = self.predict_matching(nearest_dist)
        if matched_flag:
            nearest_census_tract = self.all_geolocated_points[nearest_node_index][2]
            if verbose:
                print('matched succeeded. predicted_census_tract:', nearest_census_tract)
            return nearest_census_tract
        else:
            queried_output = get_census_tract_by_geo_info(longitude, latitude, verbose)['census_tract_id']
            print('matched failed. queried_census_tract:', queried_output['census_tract_id'])
            return queried_output['censusact_id']

    def get_census_tract_by_address(self, address, verbose=True):
        """
        find the census tract to which a given point defined by their
        longitude and latitude belongs.

        :param longitude: float

        :param latitude: float
        :param: verbose: boolean -> whether to print detailed outputs as the program runs
        :return: matched_dict: dictionary with four keys:
                                - census_block_id
                                - census_tract_id
                                - county_id
                                - state_id
        """
        geocoded_result = None
        repeated_trial = 0
        while geocoded_result is None:  # repeatly calling the Census API until the program gets the right return
            repeated_trial += 1
            if repeated_trial >= 3:
                return None
            try:
                one_line_address = ", ".join([address, self.city.title(), self.city_to_state[self.city]])
                geocoded_result = cg.onelineaddress(one_line_address)
                if len(geocoded_result) == 0:
                    geocoded_result = None
            except ValueError:
                time.sleep(random.random())
            except KeyError:
                time.sleep(random.random())
        assert len(geocoded_result)
        longitude = list(geocoded_result)[0]['coordinates']['x']
        latitude = list(geocoded_result)[0]['coordinates']['y']
        census_block_id = list(geocoded_result)[0]['geographies']['2010 Census Blocks'][0]['GEOID']
        census_tract_id = census_block_id[:-4]
        county_id = census_block_id[:5]
        state_id = census_block_id[:2]
        matched_dict = {
            'longitude': longitude,
            'latitude': latitude,
            'census_block_id': census_block_id,
            'census_tract_id': census_tract_id,
            'county_id': county_id,
            'state_id': state_id
        }
        if verbose:
            pprint(matched_dict)
        return matched_dict

    def address_geolocating(self, year=None, verbose=True):
        """
        geolocate crime incidents using the censusgeocode API and update the
        results into psql

        :param city: str -> process crime incidents that happen in one city at a time
        :param year: int -> process crime incidents that happen in one year at a time
        :param verbose: boolean -> whether to print outputs as the program runs
        :return:
        """
        if year is not None:
            query = """SELECT incident_id, address
                        FROM crime_incident
                        WHERE address IS NOT NULL
                        AND census_tract_id IS NULL
                        AND longitude IS NULL
                        AND year = %s
                        AND state = %s
                        LIMIT {}
                        ;
                        """.format(self.batch_size)
            self.crime_cursor.execute(query, (year, self.state_abbr))
        else:
            query = """SELECT incident_id, address
                        FROM crime_incident
                        WHERE address IS NOT NULL
                        AND census_tract_id IS NULL
                        AND longitude IS NULL
                        AND state = %s
                        LIMIT {}
                        ;
                        """.format(self.batch_size)
            self.crime_cursor.execute(query, (self.state_abbr, ))
        results = self.crime_cursor.fetchall()
        if len(results): # check if there are still records waiting to be geolocated
            for result in tqdm(results, total=len(results)):
                incident_id = result[0]
                address = result[1].lower().replace(' block', '')
                address = address.replace('nb', '')
                address = address.replace('sb', '')
                address = address.replace('ih', 'interstate')
                address = address.replace(' s ', ' south ')
                address = address.replace(' n ', ' north ')
                address = address.replace(' e ', ' east ')
                address = address.replace(' w ', ' west ')
                if 'interstate' in address:
                    address = address.replace('svrd', '')
                if address != 'unknown':
                    matched_dict = self.get_census_tract_by_address(address, verbose)
                    if matched_dict is not None:
                        cdb._update_census_block_to_psql(incident_id, matched_dict['census_tract_id'],
                                                     census_block_id=matched_dict['census_block_id'],
                                                     longitude=matched_dict['longitude'],
                                                     latitude=matched_dict['latitude'],
                                                     verbose=verbose)
                        self.crime_connection.commit()
        else:
            print('all the crime incidents are already labelled')
            sys.exit()

    def local_geolocating(self, year=None, verbose=True):
        """
        geolocate crime incidents using the censusgeocode API and update the
        results into psql

        :param city: str -> process crime incidents that happen in one city at a time
        :param year: int -> process crime incidents that happen in one year at a time
        :param verbose: boolean -> whether to print outputs as the program runs
        :return:
        """
        self.get_geolocated_points_by_state()
        if year is not None:
            query = """SELECT incident_id, longitude, latitude
                        FROM crime_incident
                        WHERE longitude IS NOT NULL
                        AND longitude != 'NaN'
                        AND latitude > 0
                        AND census_tract_id IS NULL
                        AND year = %s
                        AND state = %s
                        LIMIT {}
                        ;
                        """.format(self.batch_size)
            self.crime_cursor.execute(query, (year, self.state_abbr))
        else:
            query = """SELECT incident_id, longitude, latitude
                        FROM crime_incident
                        WHERE longitude IS NOT NULL
                        AND longitude != 'NaN'
                        AND latitude > 0
                        AND census_tract_id IS NULL
                        AND state = %s
                        LIMIT {}
                        ;
                        """.format(self.batch_size)
            self.crime_cursor.execute(query, (self.state_abbr, ))
        results = self.crime_cursor.fetchall()
        if len(results): # check if there are still records waiting to be geolocated
            for result in tqdm(results, total=len(results)):
                incident_id = result[0]
                longitude = result[1]
                latitude = result[2]
                if longitude < - 50 and latitude > 20:
                    #output = get_census_tract_by_geo_info(longitude, latitude, verbose)
                    census_tract_id = self.geolocate_point(longitude, latitude, verbose)
                    cdb._update_census_block_to_psql(incident_id, census_tract_id, verbose=verbose)
                    self.crime_connection.commit()
        else:
            print('all the crime incidents are already labelled')
            sys.exit()

    def _update_census_block_to_psql(self, incident_id, census_tract_id, census_block_id=None,
                                     longitude=None, latitude=None, verbose=True):
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
        if census_block_id is not None and longitude is not None:

            query = """UPDATE crime_incident
                        SET census_block_id = %s,
                            census_tract_id = %s,
                            longitude = %s,
                            latitude = %s
                        WHERE incident_id = %s 
                        RETURNING incident_id
                        """
            self.crime_cursor.execute(query, (census_block_id, census_tract_id,
                                            longitude, latitude, incident_id))
            query_output = self.crime_cursor.fetchone()
        elif census_block_id is not None:
            query = """UPDATE crime_incident
                        SET census_block_id = %s,
                            census_tract_id = %s
                        WHERE incident_id = %s 
                        RETURNING incident_id
                        """
            self.crime_cursor.execute(query, (census_block_id, census_tract_id, incident_id))
            query_output = self.crime_cursor.fetchone()
        else:
            query = """UPDATE crime_incident
                        SET census_tract_id = %s
                        WHERE incident_id = %s 
                        RETURNING incident_id
                        """
            self.crime_cursor.execute(query, (census_tract_id, incident_id))
            query_output = self.crime_cursor.fetchone()
        if verbose:
            print('updated property:', query_output)
        return True


if __name__ == '__main__':
    ## connect to database
    crime_connection = psycopg2.connect(DBInfo.crime_config)
    crime_cursor = crime_connection.cursor()
    acs5_connection = psycopg2.connect(DBInfo.acs5_config)
    acs5_cursor = acs5_connection.cursor()
    airbnb_connection = psycopg2.connect(DBInfo.airbnb_config)
    airbnb_cursor = airbnb_connection.cursor()
    ## set up CrimeDB
    cdb = CrimeDB(state_abbr='NY')  # CA, IL, MA, TX
    cdb.connect_to_db(acs5={'connection': acs5_connection, 'cursor':acs5_cursor},
                      crime={'connection': crime_connection, 'cursor': crime_cursor},
                      airbnb={'connection': airbnb_connection, 'cursor': airbnb_cursor})
    ## copy the table census tracts from acs5
    # cdb.copy_census_tracts_from_acs5()
    ## import crime data from pandas to psql
    # cdb.insert_crimes_into_psql('nyc', year_threshold=2014) # austin, boston, chicago, dc, la, nyc, seattle, sf
    # cdb.drop_crimes_by_year(2014)
    ## geolocate crime using airbnb
    cdb.batch_size = 10000
    while True:
        cdb.local_geolocating(verbose=False)
    ## geolocate TX by address
    # cdb.batch_size = 100
    # cdb.city = 'austin'
    # while True:
    #     cdb.address_geolocating(year=2019, verbose=False)
