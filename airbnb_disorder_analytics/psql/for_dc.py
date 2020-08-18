'''
test_new_ways_to_geolocate_census_tracts.py

test if we can geolocate crime incidents using existing info (geolocated airbnb and crime incidents)
idea: geolocate all crimes in 2019, use this information to predict the labels of previous years

matching is based on the longitude and latitude of the listing.

Dependencies:
    - third-party packages: psycopg2, censusgeocode
    - local packages: config.db_config, psql.match_property_to_census_tracts

ruilin chen
08/15/2020
'''
# system import
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import pickle
import random
# third-party import
import psycopg2
from scipy.spatial import cKDTree
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.config.us_states import USStates

airbnb_connection = psycopg2.connect(DBInfo.airbnb_config)
airbnb_cursor = airbnb_connection.cursor()

crime_connection = psycopg2.connect(DBInfo.crime_config)
crime_cursor = crime_connection.cursor()

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score

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
        self.clf = pickle_model

def get_records_to_geolocate(state_id): # include geolocated records in both airbnb_data and crime_data
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
    crime_cursor.execute(query, (state_id,))
    state = uss.abbr_to_name[uss.fips_to_abbr[state_id]]
    list_of_records = crime_cursor.fetchall()
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
    full_list = list_of_records + another_list_of_records
    return full_list

def ckdnearest(all_nodes, target_node, k=3):
    # k specifies the number of smallest values to return
    btree = cKDTree(target_node)
    dist, idx = btree.query(all_nodes, k=1)
    dist = dist.flatten()
    indices_of_min = np.argpartition(dist, k)[:k]
    return indices_of_min, dist[indices_of_min]

def get_census_tract_id_from_code(acs_cursor, state_abbr):
    query = """SELECT census_tract_id, census_tract_code
                FROM census_tracts
                WHERE state_abbr = %s
                ;
                """
    acs_cursor.execute(query, (state_abbr,))
    results = acs_cursor.fetchall()
    code_to_id = {item[1]: item[0] for item in results}
    return code_to_id

def insert_by_pandas(city, state_abbr, crime_df, crime_columns=None, batch_size=10000):
    """
    insert crime incidents stored in a pandas dataframe into psql

    :param city: st
    :param crime_df: a pandas dataframe with the raw data
    :param crime_columns: a dictionary with the keys being psql columns and values being the
                            corresponding pandas columns
    :return: None
    """
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
        census_tract_code = row[crime_columns['census_tract']]
        try:
            census_tract_id = code_to_id[census_tract_code]
        except KeyError:
            error_count += 1
            continue
        try:
            year = pd.to_datetime(row[crime_columns['date']]).year
        # if the provided date is out of bound, ignore the entry
        except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
            error_count += 1
            continue
        date = row[crime_columns['date']]
        # if date is not provided, print the row,
        # ignore the entry and increment the error_count by 1
        if pd.isnull(date):
            print(row)
            error_count += 1
        else:
            if pd.isnull(longitude) and pd.isnull(latitude):
                # if none of the longitude, latitude and address columns have meaningful information,
                # ignore the record and increment the error_count by 1
                error_count += 1
            else:
                query = """INSERT INTO crime_incident (incident_id, description, longitude, latitude, census_tract_id, year, city, state, date) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (incident_id) DO NOTHING
                                    RETURNING incident_id
                                    ;
                                """
                values = (incident_id, description, longitude, latitude, census_tract_id, year, city, state_abbr, date)
                crime_cursor.execute(query, values)
                if index % batch_size == 0:
                    crime_connection.commit()
    crime_connection.commit()
    print('== error count:', error_count)

def get_geolocated_points_by_state(state_abbr):  # include geolocated records in crime_data
    state_id = uss.abbr_to_fips[state_abbr]
    state = uss.abbr_to_name[state_abbr]
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
    crime_cursor.execute(query, (state_id,))
    all_geolocated_points = crime_cursor.fetchall()
    return all_geolocated_points

def get_airbnb_points_to_geolocate(state_abbr):
    state = uss.abbr_to_name[state_abbr]
    query = """SELECT property.longitude, property.latitude
                FROM property
                WHERE property.longitude IS NOT NULL
                AND property.longitude != 'NaN'
                AND property.census_tract_id IS NULL
                AND property.state = %s
                ;
            """
    airbnb_cursor.execute(query, (state, ))
    return airbnb_cursor.fetchall()

def load_classifer(pkl_name):
    with open(pkl_name, 'rb') as file:
        pickle_model = pickle.load(file)
    return pickle_model

def predict_matching(clf, input):
    output = clf.predict(np.array([input]).reshape(1, -1))
    return bool(output[0])

def geolocate_point(longitude, latitude):
    target_node = np.array([[longitude, latitude]])
    nearest_node_index, nearest_dist = ckdnearest(all_geolocated_nodes, target_node)
    matched_flag = predict_matching(nearest_dist)
    if matched_flag:
        nearest_census_tract = all_geolocated_points[nearest_node_index][2]
        print('matched succeeded. predicted_census_tract:', nearest_census_tract)
        return nearest_census_tract

if __name__ == '__main__':
    city = 'DC'
    state_abbr = 'DC' # MA: 25; TX: 48, NY: 36, CA: 06, IL: 17
    uss = USStates()
    state_id = uss.abbr_to_fips[state_abbr]
    acs5_connection = psycopg2.connect(DBInfo.acs5_config)
    acs5_cursor = acs5_connection.cursor()
    crime_connection = psycopg2.connect(DBInfo.crime_config)
    crime_cursor = crime_connection.cursor()
    airbnb_connection = psycopg2.connect(DBInfo.airbnb_config)
    airbnb_cursor = airbnb_connection.cursor()
    uss = USStates()

    data_folder = '/home/rchen/Documents/github/airbnb_crime/data/crime_data'
    crime_filenames = ['DC_Crime_Incidents_in_2019.csv',
                      'DC_Crime_Incidents_in_2018.csv',
                      'DC_Crime_Incidents_in_2017.csv',
                      'DC_Crime_Incidents_in_2016.csv',
                      'DC_Crime_Incidents_in_2015.csv',
                      'DC_Crime_Incidents_in_2014.csv'
                      ]
    crime_columns = {'incident_id': 'CCN',
                     'description': 'OFFENSE',
                     'longitude': 'LONGITUDE',
                     'latitude': 'LATITUDE',
                     'date': 'REPORT_DAT',
                     'census_tract': 'CENSUS_TRACT'
                     }
    code_to_id = get_census_tract_id_from_code(acs_cursor=acs5_cursor, state_abbr='DC')

    # for crime_file in crime_filenames:
    #     df = pd.read_csv(os.path.join(data_folder, crime_file))
    #     #print(df.columns)
    #     df['CENSUS_TRACT'] = df['CENSUS_TRACT'].astype('Int64').astype(str).str.zfill(5)
    #     insert_by_pandas(city, state_abbr, df, crime_columns=crime_columns)
    all_geolocated_points = get_geolocated_points_by_state(state_abbr)
    all_geolocated_nodes = np.array([[point[0], point[1]] for point in all_geolocated_points])
















