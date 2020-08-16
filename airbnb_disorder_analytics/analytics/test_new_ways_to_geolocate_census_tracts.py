'''
test_new_ways_to_geolocate_census_tracts.py

test if we can geolocate crime incidents using existing info (geolocated airbnb and/or crime incidents)

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
# third-party import
import psycopg2
from scipy.spatial import cKDTree
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo

connection = psycopg2.connect(DBInfo.airbnb_config)
cursor = connection.cursor()

crime_connection = psycopg2.connect(DBInfo.crime_config)
crime_cursor = crime_connection.cursor()

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split


class NearestClassifier:
    def __init__(self, X, Y, k):
        self.random_state = 123
        self.all_X = np.array(X).reshape(-1, k)
        print('X shape:', self.all_X.shape)
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
        true_positive = 0
        false_positive = 0
        for predicted_value, actual_value in zip(predicted_Y, self.valid_Y):
            if actual_value and predicted_value:
                true_positive += 1
            elif not actual_value and predicted_value:
                false_positive += 1
        accuracy_rate = true_positive / float(true_positive + false_positive)
        print('true_positive:', true_positive, 'false_positive:', false_positive,
              'accuracy rate:', accuracy_rate)
        return accuracy_rate

    def save_classifier(self, pkl_name):
        with open(pkl_name, 'wb') as file:
            pickle.dump(self.clf, file)

    def load_classifer(self, pkl_name):
        with open(pkl_name, 'rb') as file:
            pickle_model = pickle.load(file)
        self.clf = pickle_model

def get_records_to_geolocate(state_id=None):
    if state_id is not None:
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
        crime_cursor.execute(query, (str(state_id),))
        list_of_records = crime_cursor.fetchall()
        return list_of_records
    else:
        query = """SELECT crime_incident.longitude, crime_incident.latitude, crime_incident.census_tract_id
                    FROM crime_incident, census_tracts
                    WHERE crime_incident.census_tract_id = census_tracts.census_tract_id
                    AND crime_incident.longitude IS NOT NULL
                    AND crime_incident.longitude != 'NaN'
                    AND crime_incident.census_tract_id IS NOT NULL
                    AND crime_incident.census_tract_id != 'NaN'
                    ;
                    """
        crime_cursor.execute(query)
        list_of_records = crime_cursor.fetchall()
        return list_of_records

def ckdnearest(all_nodes, target_node, k=3):
    # k specifies the number of smallest values to return
    btree = cKDTree(target_node)
    dist, idx = btree.query(all_nodes, k=1)
    dist = dist.flatten()
    indices_of_min = np.argpartition(dist, k)[:k]
    return indices_of_min, dist[indices_of_min]

state_id = '17'

list_of_records = get_records_to_geolocate(state_id)
all_census_tracts = [a_record[2] for a_record in list_of_records]
all_nodes = [np.array([[a_record[0], a_record[1]]]) for a_record in list_of_records]

for i in range(3):
    number_of_features = i+1
    print('number of features:', number_of_features)
    list_of_nearest_dists = []
    list_of_matched_flag = []

    for index, a_record in tqdm(enumerate(list_of_records), total=len(list_of_records)):
        target_node = np.array([[a_record[0], a_record[1]]])
        source_nodes = all_nodes[:index] + all_nodes[index+1:]
        source_census_tracts = all_census_tracts[:index] + all_census_tracts[index+1:]
        target_census_tract = a_record[2]
        nearest_node_indices, nearest_dists = ckdnearest(source_nodes, target_node, k=number_of_features)
        nearest_census_tract = source_census_tracts[nearest_node_indices[0]]
        list_of_nearest_dists.append(nearest_dists)
        list_of_matched_flag.append(int(nearest_census_tract == target_census_tract))

    # change class_weight to penalize false_positive
    model = NearestClassifier(X=list_of_nearest_dists, Y=list_of_matched_flag, k=number_of_features)
    model.train_classifier()









