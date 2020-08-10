from airbnb_disorder_analytics.config.db_config import DBInfo
import psycopg2
import numpy as np
import os
from tqdm import tqdm

# connect to database
connection = psycopg2.connect(DBInfo.airbnb_config)
cursor = connection.cursor()

# how many review per reviewer has in the data?
query = """SELECT reviewer_id, COUNT(property_id) as review_count
            FROM review
            GROUP BY reviewer_id
            ORDER BY review_count DESC
            ;
            """
cursor.execute(query)
review_id_and_count = cursor.fetchall()
review_count = [item[1] for item in review_id_and_count]
print('count of count bigger than 5:', sum([1 for count in review_count if count >= 5]))
print('5th quantile:', np.quantile(review_count, 0.05))
print('10th quantile:', np.quantile(review_count, 0.1))
print('1th quantile:', np.quantile(review_count, 0.01))
print('mean', np.mean(review_count))

# some words related to the questionnaire that I can look for the review data.
