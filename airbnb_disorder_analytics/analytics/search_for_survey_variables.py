'''
search_for_survey_variables.py

this module allows users to enter a search query and return a number of survey variables
that best match the query.
'''

#system import
import os
import pandas as pd
from pprint import pprint
# third-party import
import psycopg2
from fuzzywuzzy import fuzz
from fuzzywuzzy import process as fuzzywuzzy_process
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo

class ACSVariableLookUp:
    def __init__(self):
        self.connection = psycopg2.connect(DBInfo.acs_config)
        self.cursor = self.connection.cursor()
        self.variable_name_and_id = self.get_variable_name_and_id()
        self.variable_names = list(self.variable_name_and_id.keys())
        self.table_name_and_id = self.get_table_name_and_id()
        self.table_names = list(self.table_name_and_id.keys())
        self.output = None
        self.output_filename = 'key_acs5_variables.csv'

    def get_variable_name_and_id(self):
        query = """SELECT variable_id, title
                    FROM survey_variable
                    ;
                    """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        variable_name_to_id = {result[1].replace('%', ' ').lower(): result[0] for result in results}
        return variable_name_to_id

    def get_variable_names_by_table(self, table_id):
        query = """SELECT title
                    FROM survey_variable
                    WHERE table_id = '{}'
                    ;
                    """.format(table_id)
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        variable_names = [result[0].replace('%', ' ').lower() for result in results]
        return variable_names

    def get_table_name_and_id(self):
        query = """SELECT table_id, title
                    FROM survey_table
                    ;
                    """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        table_name_to_id = {result[1].lower(): result[0] for result in results}
        return table_name_to_id

    def search(self, variable_query=None, table_query=None):
        print('== search entry:')
        print(f'[{variable_query}] RELATED TO [{table_query}]')
        if table_query is not None:
            matched_table, match_score = fuzzywuzzy_process.extractOne(table_query, self.table_names, scorer=fuzz.token_sort_ratio)  # or partial_ratio
            print('matched table:', matched_table)
            matched_table_id = self.table_name_and_id[matched_table]
            variables_under_matched_table = self.get_variable_names_by_table(matched_table_id)
            matched_results = fuzzywuzzy_process.extract(variable_query, variables_under_matched_table, scorer=fuzz.partial_ratio, limit=5)
            output = []
            for matched_result in matched_results:
                matched_variable_name = matched_result[0]
                output.append([matched_variable_name, self.variable_name_and_id[matched_variable_name]])
            self.output = output
            self.get_user_input()
        else:
            return False

    def get_user_input(self):
        print('== matched records:')
        for index, item in enumerate(self.output):
            print(f'-{index}    {item[0]}')
        succeeded = input('\n is any of these records what you need? enter y/n')
        if succeeded or succeeded == 'y':
            save_flag = input('do you want to save the records you need to file? enter y/n')
            if save_flag or save_flag == 'y':
                save_command = input('which of them do you want to save? enter their index/indices separated by comma or blank space')
                if ',' in save_command:
                    record_indices = [int(i) for i in save_command.split(',') if i.isdigit()]
                else:
                    record_indices = [int(i) for i in save_command.split(' ') if i.isdigit()]
                df = pd.DataFrame(columns=['variable_id', 'title'])
                df['variable_id'] = [self.output[record_index][1]for record_index in record_indices]
                df['title'] = [self.output[record_index][0] for record_index in record_indices]
                if os.path.isfile(self.output_filename):
                    old_df = pd.read_csv(self.output_filename)
                    df = pd.concat([df, old_df])
                df.to_csv(self.output_filename, index=False)
                print('saved selected records to file')
        else:
            pass

if __name__ == '__main__':
    lookup = ACSVariableLookUp()
    #lookup.search(variable_query='african american total population', table_query='race')
    #lookup.search(variable_query='household income', table_query='Median Household Income In The Past 12 Months (In 2018 Inflation-Adjusted Dollars)')
    #lookup.search(variable_query='poverty', table_query='Poverty Status In The Past 12 Months By Disability Status By Employment Status For The Population 20 To 64 Years')
    #lookup.search(variable_query='unemployment', table_query='unemployment')
    #lookup.search(variable_query='educational attainment', table_query='educational attainment')
    lookup.search(variable_query='geographical mobility', table_query='Geographical Mobility In The Past Year By Age For Current Residence In The United States')