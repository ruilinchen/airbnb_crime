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


if __name__ == '__main__':
    state_abbr = 'TX' # MA: 25; TX: 48, NY: 36, CA: 06, IL: 17
    uss = USStates()
    state_id = uss.abbr_to_fips[state_abbr]

    list_of_records = get_records_to_geolocate(state_id)
    all_census_tracts = [a_record[2] for a_record in list_of_records]
    all_nodes = [np.array([[a_record[0], a_record[1]]]) for a_record in list_of_records]

    number_of_features = 1
    list_of_nearest_dists = []
    list_of_matched_flag = []

    batch_size = 10000
    for index, a_record in tqdm(enumerate(list_of_records), total=batch_size):
        target_node = np.array([[a_record[0], a_record[1]]])
        #source_nodes = all_nodes[:index] + all_nodes[index+1:]
        target_census_tract = a_record[2]
        nearest_node_indices, nearest_dists = ckdnearest(all_nodes[:index] + all_nodes[index+1:],
                                                     target_node, k=number_of_features)
        nearest_census_tract_index = nearest_node_indices[0] + int(nearest_node_indices[0] >= index)
        nearest_census_tract = all_census_tracts[nearest_census_tract_index]
        list_of_nearest_dists.append(nearest_dists)
        list_of_matched_flag.append(int(nearest_census_tract == target_census_tract))
        if index > batch_size:
            break

    # change class_weight to penalize false_positive
    model = NearestClassifier(X=list_of_nearest_dists, Y=list_of_matched_flag, k=number_of_features)
    model.train_classifier()
    model.save_classifier(f'matching_classifier_{state_abbr}.pkl')
    print('saved model to classifier')









