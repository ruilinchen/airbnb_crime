'''
test_new_ways_to_geolocate_census_tracts.py

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
# third-party import
import psycopg2
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.psql.match_property_to_census_tracts import get_census_tract_by_geo_info
