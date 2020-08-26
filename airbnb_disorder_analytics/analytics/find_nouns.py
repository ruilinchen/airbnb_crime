'''
find_nouns.py

find more neighborhood-related nouns based on existing labelled data
'''

# system import
from tqdm import tqdm
import pandas as pd
import os
# third-party import
import psycopg2
import spacy
import jellyfish
import re
import math
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.analytics.query_reviews import QueryReview

qr = QueryReview()
qr.batch_size = 10000

file_name = 'more_nouns_to_label.csv'

if os.path.isfile(file_name):
    df = pd.read_csv(file_name)
    words_to_label = set(df['word'])
else:
    words_to_label = set()

while True:
    query = """SELECT review.reviewer_id, review.property_id, review.review_text
                FROM review, property
                WHERE review.browsed = false
                AND review.property_id = property.airbnb_property_id
                LIMIT %s
                ;
              """
    qr.airbnb_cursor.execute(query, (qr.batch_size,))
    results = qr.airbnb_cursor.fetchall()

    if len(results) == 0:
        break
    for result in tqdm(results, total=len(results)):
        reviewer_id = result[0]
        property_id = result[1]
        doc = qr.nlp(result[2])
        for token in doc:
            for labelled_noun in qr.list_of_nouns_for_neighborhood:
                word = re.sub(r'\W+', '', token.lower_)
                dist = jellyfish.damerau_levenshtein_distance(word, labelled_noun)
                if dist == 0:
                    break
                elif dist <= min(math.ceil(len(word)/4), 2):
                    words_to_label.add(token.lower_)
                    break

        a_query = """UPDATE review 
                        SET browsed = true
                        WHERE reviewer_id = %s 
                        AND property_id = %s;"""
        qr.airbnb_cursor.execute(a_query, (reviewer_id, property_id))
    qr.airbnb_connection.commit()
    df = pd.DataFrame(columns=['word', 'is_neighbor'])
    df['word'] = list(words_to_label)
    df['is_neighbor'] = 0
    df.to_csv(file_name, index=False)
    print('found nouns to label:', len(words_to_label))



