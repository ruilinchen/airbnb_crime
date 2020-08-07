from airbnb_disorder_analytics.config.db_config import DBInfo
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


# raw data files
region_list = ['austin-round-rock-tx',
               'boston-cambridge-newton-ma-nh']

target_region = 'austin-round-rock-tx'

target_filename_dict = get_filenames_by_region(target_region)

# todo update property table
property_filename = target_filename_dict['property']
property_df = pd.read_csv(os.path.join('../data', property_filename))


def insert_into_property(property_df):
    '''
    database function -- populate the property table in airbnb_data

    :param property_df: a pandas dataframe
    :return: ???
    '''

    db_columns = ['property_id', 'property_title', 'property_type',
                    'listing_type', 'created_on', 'last_scraped_on',
                    'country', 'latitude', 'longitude', 'state',
                    'city', 'zipcode', 'neighborhood', 'msa',
                    'average_daily_rate', 'annual_revenue',
                    'occupancy_rate', 'number_of_bookings',
                    'count_reservation_days', 'count_available_days',
                    'count_blocked_days', 'calendar_last_updated',
                    'response_rate', 'airbnb_superhost',
                    'security_deposit', 'cleaning_fee', 'published_nightly_fee',
                    'published_monthly_rate', 'published_weekly_rate', 'number_of_reviews',
                    'overall_rating', 'airbnb_host_id', 'airbnb_listing_url'
                  ]  # a list of all the column headings to be included in the insertion entry
    df_columns = ['Property ID', 'Listing Title', 'Property Type',
                  'Listing Type', 'Created Date', 'Last Scraped Date',
                  'Country', 'Latitude', 'Longitude',
                  'State','City','Zipcode', 'Neighborhood',
                  'Metropolitan Statistical Area', 'Average Daily Rate (USD)',
                  'Annual Revenue LTM (USD)', 'Occupancy Rate LTM',
                  'Number of Bookings LTM', 'Count Reservation Days LTM',
                  'Count Available Days LTM', 'Count Blocked Days LTM',
                  'Calendar Last Updated', 'Response Rate', 'Airbnb Superhost',
                  'Security Deposit (USD)', 'Cleaning Fee (USD)',
                  'Published Nightly Rate (USD)', 'Published Monthly Rate (USD)',
                  'Published Weekly Rate (USD)', 'Number of Reviews', 'Overall Rating',
                  'Airbnb Host ID', 'Airbnb Listing URL'
                  ] # a list of pandas column names arranged in the same sequence as the database columns list
    for row in tqdm(property_df.loc[:, df_columns].itertuples()):
        print(row)
        break
        query = ''' INSERT INTO %s('%s') VALUES(%%s); ''' % (table_name, ', '.join(db_columns))
        cursor.execute(query, row)
    return False


insert_into_property(property_df)


## todo update daily_booking table

## todo update monthly_match table


## todo update reviewer and review table
