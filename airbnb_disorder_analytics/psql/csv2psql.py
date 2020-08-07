from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.config.csv2psql_config import ColumnInfo
import psycopg2
import pandas as pd
import os
from tqdm import tqdm

# connect to database
connection = psycopg2.connect(DBInfo.psycopg2_config)
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


def insert_into_database(df, column_dict, verbose=True):
    '''
    database function -- populate the property table in airbnb_data

    :param property_df: a pandas dataframe
    :return: True
    '''
    step = 0
    for row in tqdm(df.loc[:, column_dict['df']].itertuples(index=False), total=len(df)):
        records_list_template = ','.join(['%s'] * len(row))
        insert_query = """INSERT INTO {table} ({columns})
                            VALUES ({values}) 
                            ON CONFLICT ({keys}) DO NOTHING
                            RETURNING {output} ;""".format(table=column_dict['table'],
                                                              columns=','.join(column_dict['db']),
                                                              values=records_list_template,
                                                              keys=','.join(column_dict['primary_keys']),
                                                              output=column_dict['primary_keys'][0])
        cursor.execute(insert_query, tuple(row))
        if verbose:
            print(cursor.fetchone())
    connection.commit()
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
    return True

region_list = ['austin-round-rock-tx', 'boston-cambridge-newton-ma-nh',
               'chicago-naperville-elgin-il-in-wi', ]
target_region = 'boston-cambridge-newton-ma-nh'
target_filename_dict = get_filenames_by_region(target_region)

monthly_match_filename = target_filename_dict['monthly_match']
monthly_match_df = pd.read_csv(os.path.join('../../data', monthly_match_filename))
print('finished reading monthly match')
insert_into_database(monthly_match_df, ColumnInfo.monthly_match, verbose=False)

