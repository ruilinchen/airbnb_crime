"""
test_link_between_block_code_between_block_group.py

test if the census_block_group can be extracted by taking the first digit of census_block_code

ruilin
08/10/2020
"""

# system import
import os
import sys
import pandas as pd
from pprint import pprint
# third-party import
import psycopg2
from tqdm import tqdm
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.config.us_states import USStates
from airbnb_disorder_analytics.psql.build_acs_database import ACSCensusTract

os.chdir('../psql')
# connection = psycopg2.connect(DBInfo.acs_config)
# cursor = connection.cursor()
#
# query = """SELECT census_block_code, census_tract_id, state_id, state_abbr
#             FROM census_blocks
#             GROUP BY state_id, state_abbr, census_tract_id, census_block_code
#             ORDER BY state_id, census_tract_id, census_block_code DESC
#             LIMIT 10
#             ;
#             """
# cursor.execute(query)
# results = cursor.fetchall()
# print(results[0][2:])
# block_group_by_tract = {}
# for result in results:
#     tract_id = result[1]
#     block_code = result[0]
#     block_group_id = block_code[0]
#     block_group_by_tract[tract_id] = block_group_by_tract.get(tract_id, []) + [block_group_id]
# for key, value in block_group_by_tract.items():
#     block_group_by_tract[key] = set(value)
# pprint(block_group_by_tract)

class CensusTest(ACSCensusTract):
    def __init__(self, year, state_abbr):
        super(CensusTest, self).__init__(year, state_abbr)

    def get_block_group_code_from_psql(self):
        query = """SELECT census_tract_id, census_block_code
                    FROM census_blocks
                    WHERE state_abbr = %s
                    GROUP BY census_tract_id, census_block_code
                    ;
                    """
        self.cursor.execute(query, (self.state_abbr, ))
        results = self.cursor.fetchall()
        return [(result[0], result[1][0]) for result in results]  # the first digit of census_block_code

    def test(self):
        self.process_raw_data()
        geo_template = self.templates['geo']
        gdf = self.read_from_csv(os.path.join(self.data_folder, self.summary_foldername, self.gfilename),
                                 names=geo_template)
        gdf = gdf[['Logical Record Number', 'Geographic Identifier']][
            gdf['Summary Level'] == self.summary_level_for_census_block]
        gdf.columns = ['logical_record_number', 'geographic_identifier']
        geographic_identifier_list = list(gdf['geographic_identifier'])
        block_group_id_list = [item.split('US')[1] for item in geographic_identifier_list]
        block_group_code_list = [block_group_id[-1] for block_group_id in block_group_id_list]
        census_tract_id_list = [block_group_id[:-1] for block_group_id in block_group_id_list]
        gdf['block_group_code'] = block_group_code_list
        gdf['census_tract_id'] = census_tract_id_list
        tract_to_block_group_codes = {}
        for index, row in gdf.iterrows():
            tract_to_block_group_codes[row['census_tract_id']] = tract_to_block_group_codes.get(row['census_tract_id'], []) + [row['block_group_code']]
        print('began testing for', self.state_abbr)
        tract_id_and_group_code = self.get_block_group_code_from_psql()
        tract_id_to_block_groups = {}
        failed_count = 0
        for tract_id, group_code in tract_id_and_group_code:
            tract_id_to_block_groups[tract_id] = tract_id_to_block_groups.get(tract_id, []) + [group_code]
        for tract_id, block_group_code_list in tract_to_block_group_codes.items():
            # print(tract_id)
            block_group_code_set = set(block_group_code_list)
            if tract_id not in tract_id_to_block_groups:
                print('tract_id not found', tract_id)
                continue
            block_group_code_set_in_psql = set(tract_id_to_block_groups[tract_id])
            # print(len(block_group_code_set), len(block_group_code_set_in_psql))
            if block_group_code_set != block_group_code_set_in_psql:
                print(block_group_code_set, block_group_code_set_in_psql)
                failed_count += 1
        if failed_count == 0:
            print('passed test')
        else:
            print(f'failed test {failed_count} times')

ct = CensusTest(year=2018, state_abbr='CA')
list_of_us_states = ct.uss.all_states(abbr=True)  # can loop through this list
ct.test()
for us_state in list_of_us_states:
    if us_state == 'NY':
        continue
    ct.set_new_state(state_abbreviation=us_state)
    ct.test()
