'''
match_property_to_census_tracts.py

this script includes functions that match airbnb listings to their corresponding neighborhoods defined
as the census tracts. it is also defined to automatically save all the matching results to database by
updating the collection "census_tract" and "census_block" in database "airbnb_data" and adding
new columns called "census_tract_id" and "census_block_id" to each of the items in collection "property".

matching is based on the longitude and latitude of the listing.

Dependencies:
    - third-party packages: psycopg2, censusgeocode
    - local packages: config.db_config

ruilin chen
08/09/2020
'''

# system import
import time
import random
from pprint import pprint
from tqdm import tqdm
import multiprocessing
# third-party import
import psycopg2
import censusgeocode as cg
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo


# connect to database
connection = psycopg2.connect(DBInfo.airbnb_config)
cursor = connection.cursor()


def get_unlocated_properties(state=None, num_of_properties=10):
    """
    get longitude and latitude for listings that are yet to be geolocated,
    meaning that the census tracts in which they are located have not been identified.

    :param num_of_properties: int -> how many properties to return
    :return: list_of_properties: list -> [(property_id, longitude, latitude)...]
    """
    if state is None:
        query = """SELECT property_id, longitude, latitude
                        FROM property
                        WHERE census_tract_id IS NULL 
                        LIMIT {}
                        ;
                    """.format(num_of_properties)
        cursor.execute(query)
        list_of_properties = cursor.fetchall()
        return list_of_properties
    else:
        query = """SELECT property_id, longitude, latitude
                        FROM property
                        WHERE census_tract_id IS NULL 
                        AND state = %s
                        LIMIT {}
                        ;
                    """.format(num_of_properties)
        cursor.execute(query, (state, ))
        list_of_properties = cursor.fetchall()
        return list_of_properties



def get_census_tract_by_geo_info(longitude, latitude, verbose=True):
    """
    find the census tract to which a given point defined by their
    longitude and latitude belongs.

    :param longitude: float
    :param latitude: float
    :param: verbose: boolean -> whether to print detailed outputs as the program runs
    :return: matched_dict: dictionary with four keys:
                            - census_block_id
                            - census_tract_id
                            - county_id
                            - state_id
    """
    geocoded_result = None
    repeated_trial = 0
    while geocoded_result is None: # repeatly calling the Census API until the program gets the right return
        repeated_trial += 1
        if repeated_trial > 10:
            break
            sys.exit()
        try:
            geocoded_result = cg.coordinates(x=longitude, y=latitude)
        except ValueError:
            time.sleep(random.random())
        except KeyError:
            time.sleep(random.random())
    assert len(geocoded_result)
    census_block_id = geocoded_result['2010 Census Blocks'][0]['GEOID']
    census_tract_id = geocoded_result['Census Tracts'][0]['GEOID']
    county_id = geocoded_result['Counties'][0]['GEOID']
    state_id = geocoded_result['States'][0]['GEOID']
    matched_dict = {
        'census_block_id': census_block_id,
        'census_tract_id': census_tract_id,
        'county_id': county_id,
        'state_id': state_id
    }
    if verbose:
        pprint(matched_dict)
    return matched_dict

def update_census_block_to_psql(property_id, census_block_id, census_tract_id, verbose=True):
    """
    insert the matching result between airbnb property and their census block info into psql
    as census block is defined to be child of census tract, the update also requires information
    on the census tract to which this block belongs.

    :param property_id: str
    :param census_block_id: str
    :param census_tract_id: str
    :param: verbose: boolean -> whether to print detailed outputs as the program runs
    :return: True
    """
    query = """UPDATE property
                SET census_block_id = %s,
                    census_tract_id = %s
                WHERE property_id = %s 
                RETURNING property_id
                """
    cursor.execute(query, (census_block_id, census_tract_id, property_id))
    query_output = cursor.fetchone()
    if verbose:
        print('updated property:', query_output)

    query = """INSERT INTO census_block (census_block_id, census_tract_id) 
                VALUES (%s, %s)
                ON CONFLICT (census_block_id) DO NOTHING
                RETURNING census_block_id
                ;
                """
    cursor.execute(query, (census_block_id, census_tract_id))
    query_output = cursor.fetchone()
    if verbose:
        print('updated census block:', query_output)
    return True


def update_census_tract_to_psql(census_tract_id, county_id, state_id, verbose=True):
    """
    insert census tract into psql
    as census tract is defined to be child of county and state, the update also requires information
    on the county and state to which this census tract belongs.

    :param census_tract_id: str
    :param county_id: str
    :param state_id: str
    :param: verbose: boolean -> whether to print detailed outputs as the program runs
    :return: True
    """
    query = """INSERT INTO census_tract (census_tract_id, county_id, state_id) 
                VALUES (%s, %s, %s)
                ON CONFLICT (census_tract_id) DO NOTHING
                RETURNING census_tract_id
                ;
                """
    cursor.execute(query, (census_tract_id, county_id, state_id))
    query_output = cursor.fetchone()
    if verbose:
        print('updated census tract:', query_output)
    return True


def geolocate_properties_by_batch(state, batch_size=10, verbose=True):
    """
    geolocate unlocated properties through the following steps:
        - get geo-info of the unlocated properties from database
        - find the corresponding census tract using the geo-info
        - update the database with the matched result

    :param: batch_size: int -> number of properties to process per batch
                                    # this batch processing setup avoids storing too much info in memory
    :param: verbose: boolean -> whether to print detailed outputs as the program runs
    :return: True
    """
    list_of_unlocated_properties = get_unlocated_properties(state, batch_size)
    for property_id, longitude, latitude in tqdm(list_of_unlocated_properties):
        matched_result = get_census_tract_by_geo_info(longitude, latitude, verbose)
        census_block_id = matched_result['census_block_id']
        census_tract_id = matched_result['census_tract_id']
        county_id = matched_result['county_id']
        state_id = matched_result['state_id']
        update_census_block_to_psql(property_id, census_block_id, census_tract_id, verbose)
        update_census_tract_to_psql(census_tract_id, county_id, state_id, verbose)
        connection.commit()


def geolocate_all_properties(state, verbose=True):
    """
    geolocate all unlocated properties by calling geolocate_properties_by_batch() until
    count of unlocated properties equal to zero

    :param: verbose: boolean -> whether to print detailed outputs as the program runs
    :return: True
    """
    if state is None:
        query = """SELECT COUNT(*) 
                    FROM property
                    WHERE census_block_id IS NULL 
                    ;
                    """ # this query counts the number of unlocated properties in psql
        cursor.execute(query)
        result = cursor.fetchone()
        count_of_unlocated_properties = result[0]
    else:
        query = """SELECT COUNT(*) 
                    FROM property
                    WHERE census_block_id IS NULL 
                    AND state = %s
                    ;
                    """ # this query counts the number of unlocated properties in psql
        cursor.execute(query, (state, ))
        result = cursor.fetchone()
        count_of_unlocated_properties = result[0]
    while count_of_unlocated_properties:
        print('remaining unlocated properties:', count_of_unlocated_properties)
        batch_size = min(count_of_unlocated_properties, 200)
        geolocate_properties_by_batch(state, batch_size, verbose)
        count_of_unlocated_properties -= batch_size


if __name__ == '__main__':
    ## connect to database
    crime_connection = psycopg2.connect(DBInfo.crime_config)
    crime_cursor = crime_connection.cursor()
    acs5_connection = psycopg2.connect(DBInfo.acs5_config)
    acs5_cursor = acs5_connection.cursor()
    airbnb_connection = psycopg2.connect(DBInfo.airbnb_config)
    airbnb_cursor = airbnb_connection.cursor()
    ## set up CrimeDB
    cdb = CrimeDB(state_abbr='NY')  # CA, IL, MA
    cdb.connect_to_db(acs5={'connection': acs5_connection, 'cursor':acs5_cursor},
                      crime={'connection': crime_connection, 'cursor': crime_cursor},
                      airbnb={'connection': airbnb_connection, 'cursor': airbnb_cursor})
    ## copy the table census tracts from acs5
    # cdb.copy_census_tracts_from_acs5()
    ## import crime data from pandas to psql
    # cdb.insert_crimes_into_psql('nyc', year_threshold=2014) # austin, boston, chicago, dc, la, nyc, seattle, sf
    # cdb.drop_crimes_by_year(2014)
    ## geolocate crime using airbnb
    cdb.batch_size = 10000
    while True:
        #cdb.local_geolocating(year=2019, verbose=False)
        cdb.local_geolocating(verbose=False)