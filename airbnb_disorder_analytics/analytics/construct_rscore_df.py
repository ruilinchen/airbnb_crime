from tqdm import tqdm
import pandas as pd
import os
# third-party import
import psycopg2
import spacy
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.analytics.query_reviews import QueryReview


qr = QueryReview()
qr.batch_size = 10000

property_dict = {}

while True:
    query = """SELECT property.latitude, property.longitude, review.positive_neighbor, review.negative_neighbor,
                    review.reviewer_id, review.property_id, review.review_text
                FROM review, property
                WHERE review.tmp_flag = false
                AND review.property_id = property.airbnb_property_id
                AND review.positive_neighbor != -1
                LIMIT %s
                ;
                """
    qr.airbnb_cursor.execute(query, (qr.batch_size, ))
    results = qr.airbnb_cursor.fetchall()
    if len(results) == 0:
        break
    for result in tqdm(results, total=len(results)):
        latitude = result[0]
        longitude = result[1]
        positive_neighbor = result[2]
        negative_neighbor = result[3]
        reviewer_id = result[4]
        property_id = result[5]
        if property_id in property_dict:
            property_dict[property_id]['negative'] += negative_neighbor
            property_dict[property_id]['positive'] += int(positive_neighbor and not negative_neighbor)
            property_dict[property_id]['count'] += 1
        else:
            property_dict[property_id] = {'negative': negative_neighbor, 'positive': int(positive_neighbor and not negative_neighbor),
                                          'count': 1, 'long': longitude, 'lat': latitude}

lat_list = []
long_list = []
score_list = []
property_id_list = []
for key in property_dict.keys():
    property_id_list.append(key)
    lat_list.append(property_dict[key]['lat'])
    long_list.append(property_dict[key]['long'])
    positive_count = property_dict[key]['positive']
    negative_count = property_dict[key]['negative']
    neighbor_count = property_id[key]['count']
    score = float(positive_count - negative_count) / neighbor_count
    score_list.append(score)
df = pd.DataFrame(columns=['lat', 'long', 'r-score', 'property_id'])
df['lat'] = lat_list
df['long'] = long_list
df['r-score'] = score_list
df['property_id'] = property_id_list
df.to_csv('rscore_nyc.csv', index=False)



