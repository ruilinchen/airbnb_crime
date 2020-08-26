'''
csv2psql.py

this script migrates the raw data from csv to Postgresql
before running it, users are expected to create a database with a set of collections
    - property
    - monthly match
    - review
    - reviewer
and set up connections to the database by modifying config.db_config.DBInfo.

for now, the program doesn't allow insertion of the daily_booking data.
to allow this, users need to include necessary information in config.csv2psql_config.ColumnInfo

Dependencies:
    - third-party packages: psycopg2
    - local packages: config.db_config and config.csv2psql_config

ruilin chen
08/09/2020
'''
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.config.csv2psql_config import ColumnInfo
import psycopg2
import pandas as pd
import os
from tqdm import tqdm
import sys
import math

# connect to database
connection = psycopg2.connect(DBInfo.airbnb_config)
cursor = connection.cursor()


def get_filenames_by_region(region):
    '''
    this function takes region as input and return the corresponding raw
    data files associated with this region

    :param region: str
    :return: a dictionary
    '''
    review_file = 'MSA_{}_Airbnb_Review_2020-02-10.csv'.format(region)
    daily_booking_file = 'MSA_{}_Daily_Match_2020-02-10.csv'.format(region)
    monthly_match_file = 'MSA_{}_Monthly_Match_2020-02-10.csv'.format(region)
    property_file = 'MSA_{}_Property_Match_2020-02-10.csv'.format(region)
    return {
        'review': review_file,
        'daily_booking': daily_booking_file,
        'monthly_match': monthly_match_file,
        'property': property_file
    }

# todo insert airbnb into property

def insert_into_database(a_df, column_dict, verbose=True):
    '''
    database function -- populate the property table in airbnb_data

    :param a_df: a pandas dataframe
    :param column_dict: a dictionary with info on matching pandas with psql tables
    :param verbose: boolean -> whether to print outputs for this function
    :return: True
    '''
    for row in tqdm(a_df.loc[:, column_dict['df']].itertuples(index=False), total=len(a_df)):
        records_list_template = ','.join(['%s'] * len(row))
        insert_query = """INSERT INTO {table} ({columns})
                            VALUES ({values}) 
                            ON CONFLICT ({keys}) DO NOTHING
                            RETURNING {output} ;""".format(table=column_dict['table'],
                                                           columns=','.join(column_dict['db']),
                                                           values=records_list_template,
                                                           keys=','.join(column_dict['primary_keys']),
                                                           output=column_dict['primary_keys'][0])
        try:
            cursor.execute(insert_query, tuple(row))
            if verbose:
                print(cursor.fetchone())
            connection.commit()
        except (Exception, psycopg2.Error) as error:
            print('\nerror:', error)
            print(tuple(row))
            if connection:
                connection.rollback()  # rollback to previous commit so that this insertion entry is abandoned and
                # does not influence later updates
    return True


def set_null_values_in_daily_booking(null_value='1990-01-01'):
    """
    database function: use NULL as opposed to '1990-01-01' to represent null values in daily booking

    during insertion, null values in pandas were replaced with '1990-01-01' because psycopg2's insert module
    does not accept empty strings in TimeStamp columns.
    this function resets these values to null and is to be used after running the insertion function.

    :param null_value: the value previously used to replace empty strings in daily_booking.csv
    :return: True
    """
    query = """UPDATE daily_booking
                SET booked_date = NULL
                WHERE booked_date = '{}';""".format(null_value)

    cursor.execute(query)
    connection.commit()

    query = """UPDATE property
                SET created_on = NULL
                WHERE created_on = '{}';""".format(null_value)
    cursor.execute(query)
    connection.commit()

    query = """UPDATE property
                SET last_scraped_on = NULL
                WHERE last_scraped_on = '{}';""".format(null_value)
    cursor.execute(query)
    connection.commit()

    query = """UPDATE reviewer
                SET member_since = NULL
                WHERE member_since = '{}';""".format(null_value)
    cursor.execute(query)
    connection.commit()
    return True

def add_airbnb_property_id_in_property(df):
    for index, row in df.iterrows():
        property_id = row['Property ID']
        if math.isnan(row['Airbnb Property ID']):
            continue
        else:
            airbnb_id = int(row['Airbnb Property ID'])
        query = """UPDATE property
                    SET airbnb_property_id = %s
                    WHERE property_id = %s;"""
        cursor.execute(query, (airbnb_id, property_id))
    connection.commit()


def count_entries(msa):
    query = """SELECT count(review.review_text)
                    FROM review, property 
                    WHERE review.property_id = property.airbnb_property_id
                    AND property.msa = %s
                    ;"""
    cursor.execute(query, (msa,))
    result = cursor.fetchone()
    return result[0]

region_dict = {'austin-round-rock-tx': 'Austin-Round Rock, TX Metro Area',
               'boston-cambridge-newton-ma-nh': 'Boston-Cambridge-Newton, MA-NH Metro Area',
               'chicago-naperville-elgin-il-in-wi': 'Chicago-Naperville-Elgin, IL-IN-WI Metro Area',
               'los-angeles-long-beach-anaheim-ca': 'Los Angeles-Long Beach-Anaheim, CA Metro Area',
               'miami-fort-lauderdale-west-palm-beach-fl': 'Miami-Fort Lauderdale-West Palm Beach, FL Metro Area',
               'new-york-newark-jersey-city-ny-nj-pa': 'New York-Newark-Jersey City, NY-NJ-PA Metro Area',
               'san-diego-carlsbad-ca': 'San Diego-Carlsbad, CA Metro Area',
               'san-francisco-oakland-hayward-ca':'San Francisco-Oakland-Hayward, CA Metro Area',
               'seattle-tacoma-bellevue-wa': 'Seattle-Tacoma-Bellevue, WA Metro Area',
               'washington-arlington-alexandria-dc-va-md-wv': 'Washington-Arlington-Alexandria, DC-VA-MD-WV Metro Area'
               }
for target_region, msa in region_dict.items():
    target_filename_dict = get_filenames_by_region(target_region)
    print(target_region)
    # filename = target_filename_dict['monthly_match']
    filename = target_filename_dict['review']
    df = pd.read_csv(os.path.join('/home/rchen/Documents/github/airbnb_crime/data', filename), error_bad_lines=False)
    existing_records = count_entries(msa)
    raw_records = df.shape[0]
    print('- raw:', raw_records, 'existing:', existing_records)
    if existing_records / raw_records > 0.9:
        continue
    # df['Airbnb Superhost'] = df['Airbnb Superhost'].fillna('')  # for property
    # df['Last Scraped Date'] = df['Last Scraped Date'].fillna('1990-01-01')  # for property
    # df['Created Date'] = df['Created Date'].fillna('1990-01-01')  # for property
    df['Member Since'] = df['Member Since'].fillna('1990-01-01')  # for review
    print('- finished reading file')
    insert_into_database(df, ColumnInfo.review, verbose=False)
    # add_airbnb_property_id_in_property(df)
    # print('- finished inserting airbnb property ids')


# replace 1990-01-01 with null in the database
# columns to look: last_scraped_on, created_on in property and member_since in review
