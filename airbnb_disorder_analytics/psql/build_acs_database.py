'''
build_acs_database.py

this program builds a database for American Community Survey 5-year Estimates by Census Tracts

ruilin
08/09/2020
'''

# system import
import os, sys
import requests, zipfile, io
import pandas as pd
# third-party import
import psycopg2
from tqdm import tqdm
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.config.us_states import USStates


class ACSCensusTract:
    def __init__(self, year, state_abbr):
        self.data_folder = 'acs5_data'
        if not os.path.isdir(self.data_folder):
            os.mkdir(self.data_folder)
        self.year = year
        self.state_abbr = state_abbr
        self.uss = USStates()
        self.state_full = self.uss.abbr2name(state_abbr,'')  # get the state name by state abbreviation in format: New York -> NewYork
        self.acs_base_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/{}'.format(self.year)
        self.summary_foldername = f'{self.state_full}_Tracts_Block_Groups_Only'
        self.templates_foldername = f'{self.year}_5yr_Summary_FileTemplates'
        self.census_block_foldername = f'census_blocks'
        self.appendix_filename = f'ACS_{self.year}_SF_5YR_Appendices.xls'
        self.table_desc_filename = f'ACS5_{self.year}_table_descriptions.csv'
        self.census_block_base_url = r'https://transition.fcc.gov/form477/Geo/CensusBlockData/CSVFiles'
        # https://transition.fcc.gov/form477/Geo/CensusBlockData/CSVFiles/District%20of%20Columbia.zip
        self.templates = {}
        self.appx_A = None
        self.efiles = {}
        self.mfiles = {}
        self.connection = psycopg2.connect(DBInfo.acs_config)
        self.cursor = self.connection.cursor()

    @staticmethod
    def stderr_print(*args, **kwargs):
        """
        print out error messages
        designed to use in try and except to catch exceptions
        """
        print(*args, **kwargs, file=sys.stderr, flush=True)

    @staticmethod
    def download_and_unzip_zip_file(zip_url, path_to_unzipped_folder):
        try:
            print(f'Requesting zip file {zip_url}')
            response = requests.get(zip_url, timeout=3.333)
            response.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall(path_to_unzipped_folder)
            print('downloaded zip file and unzipped the data to folder', path_to_unzipped_folder)
        except requests.exceptions.RequestException as e:
            ACSCensusTract.stderr_print(f'Error: Download from {zip_url} failed. Reason: {e}')

    @staticmethod
    def download_file(url, path_to_file):
        try:
            response = requests.get(url, timeout=3.333)
            response.raise_for_status()
            with open(path_to_file, 'wb') as f:
                f.write(response.content)
                print('downloaded excel file', path_to_file)
        except requests.exceptions.RequestException as e:
            ACSCensusTract.stderr_print(f'Error: Download from {url} failed. Reason: {e}')

    def set_new_state(self, state_abbr):
        self.state_abbr = state_abbr
        self.state_full = self.uss.abbr2name(state_abbr, '')
        self.summary_foldername = f'{self.state_full}_Tracts_Block_Groups_Only'

    def download_raw_data_by_state(self):
        """
        download raw data of ACS 5-year estimates for a certain state and year from Census.gov
        include three files:
            - summary zip
            - templates zip
            - appendix excel
        """

        # https://www2.census.gov/programs-surveys/acs/summary_file/2018/data/5_year_by_state/DistrictOfColumbia_All_Geographies_Not_Tracts_Block_Groups.zip
        summary_zip_url = '/'.join(
            [self.acs_base_url, 'data/5_year_by_state', self.summary_foldername + '.zip'])
        appendix_file_url = '/'.join(
            [self.acs_base_url, 'documentation/tech_docs', self.appendix_filename])
        templates_zip_url = '/'.join([self.acs_base_url, 'data', self.templates_foldername + '.zip'])
        census_block_zip_url = '/'.join(
            [self.census_block_base_url, self.uss.abbr2name(self.state_abbr, '%20') + '.zip'])
        # Download zip files, as necessary and unzip them to the data folder
        if not os.path.isdir(
                os.path.join(self.data_folder, self.summary_foldername)):  # download and unzip summary zip file
            self.download_and_unzip_zip_file(summary_zip_url, os.path.join(self.data_folder, self.summary_foldername))
        if not os.path.isdir(
                os.path.join(self.data_folder, self.templates_foldername)):  # download and unzip templates zip file
            self.download_and_unzip_zip_file(templates_zip_url,
                                             os.path.join(self.data_folder, self.templates_foldername))
        if not os.path.isfile(os.path.join(self.data_folder,
                                          self.census_block_foldername, self.state_abbr+'.csv')):  # download and unzip census block zip file
            self.download_and_unzip_zip_file(census_block_zip_url,
                                             os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}'))
            # reorganize census_block_by_state files and store all of them in a folder called "census_blocks"
            # by first moving the file into the new folder and then rename the file to "{state_abbr}.csv"
            if not os.path.isdir(os.path.join(self.data_folder, self.census_block_foldername)):
                os.mkdir(os.path.join(self.data_folder, self.census_block_foldername)) # create a folder called "census_blocks"
            census_block_filename = None
            for file in os.listdir(os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}')):
                if '.csv' in file:
                    census_block_filename = file
                # move the file to "census_blocks" and rename it to "{state_abbr}.csv"
                os.rename(os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}', census_block_filename),
                          os.path.join(self.data_folder, self.census_block_foldername, self.state_abbr+'.csv'))
                os.rmdir(os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}')) # remove the "census_block_{state}" folder

        # Download Excel files
        if not os.path.isfile(os.path.join(self.data_folder, self.appendix_filename)):
            self.download_file(appendix_file_url, os.path.join(self.data_folder, self.appendix_filename))


    def process_appendix(self):
        with open(os.path.join(self.data_folder, self.appendix_filename), 'rb') as r:
            appx_A = pd.read_excel(r, converters={'Summary File Sequence Number': str})
            appx_A.columns = ['name', 'title', 'restr', 'seq', 'start_end', 'topics', 'universe']
            # appx_A = appx_A[appx_A['restr'].str.contains('No Blockgroups') == False]
            try:
                appx_A[['start', 'end']] = appx_A['start_end'].str.split('-', 1, expand=True)
                appx_A['start'] = pd.to_numeric(appx_A['start'])
                appx_A['end'] = pd.to_numeric(appx_A['end'])
                self.appx_A = appx_A
            except ValueError as e:
                self.stderr_print(f'{e}')
                self.stderr_print(
                    f'File {os.path.join(self.data_folder, self.appendix_filename)} is corrupt or has invalid format')
                raise SystemExit(f'Exiting {__file__}')
        # print('read appx_A into class')
        if not os.path.isfile(os.path.join(self.data_folder, self.table_desc_filename)):
            table_df = self.appx_A.filter(['name', 'title'],
                                          axis=1)  # get survey table ids and their descriptions, save them to csv
            table_df.to_csv(os.path.join(self.data_folder, self.table_desc_filename), index=False)
            print('saved table descriptions to csv')


    def process_templates(self):
        self.templates = {}
        for filename in os.listdir(os.path.join(self.data_folder, self.templates_foldername)):
            key = None
            if 'seq' in filename.lower():
                # Generate 4-digit sequence number string
                index = filename.lower().index('seq')
                # Drop 'seq' and separate sequence number from file extension
                s = filename.lower()[index + 3:].split('.')[0]
                # Generate number string
                key = s.zfill(4)
            elif 'geo' in filename.lower():
                key = 'geo'
            else:
                # skip directories or other files
                continue
            df = pd.read_excel(os.path.join(self.data_folder, self.templates_foldername, filename))
            # Extract column names from data row 0
            self.templates[key] = df.loc[0].tolist()


    def process_summary(self):
        e = [f for f in os.listdir(os.path.join(self.data_folder, self.summary_foldername)) if f.startswith('e')]
        # Pull sequence number from file name positions 8-11; use as dict key
        self.efiles = {f[8:12]: f for f in e}
        # Get Margin-of-Error file names
        m = [f for f in os.listdir(os.path.join(self.data_folder, self.summary_foldername)) if f.startswith('m')]
        # Pull sequence number from file name positions 8-11; use as dict key
        self.mfiles = {f[8:12]: f for f in m}

    def _count_census_blocks_by_state(self):
        query = """SELECT COUNT(*)
                    FROM census_blocks
                    WHERE state_abbr = '{}'
                    ;
                    """.format(self.state_abbr)
        self.cursor.execute(query)
        census_block_count = self.cursor.fetchone()[0]
        return census_block_count

    def process_census_blocks(self):
        block_df = pd.read_csv(os.path.join(self.data_folder, self.census_block_foldername, self.state_abbr+'.csv'))
        existing_census_block_count = self._count_census_blocks_by_state()
        if block_df.shape[0] > existing_census_block_count:
            print('start inserting into census_blocks')
            for index, row in tqdm(block_df.iterrows(), total=len(block_df)):
                state_id = str(row['state']).zfill(2)
                county_id = str(row['county']).zfill(2)
                county_name = row['cnamelong']
                census_tract_id = str(row['tractcode']).zfill(11)  # _id is the long format
                census_block_id = str(row['blockcode']).zfill(15)  # _id is the long format
                census_tract_code = str(row['tract']).zfill(5)  # _code is the short format
                census_block_code = str(row['block']).zfill(4)  # _code is the short format
                query = """INSERT INTO census_blocks (census_block_id, census_block_code, census_tract_id, census_tract_code, 
                                                        state_id, county_id, county_name, state_abbr, state)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                                ON CONFLICT (census_block_id) DO NOTHING
                                RETURNING census_block_id 
                                ;"""
                self.cursor.execute(query, (census_block_id, census_block_code, census_tract_id, census_tract_code,
                                    state_id, county_id, county_name, self.state_abbr, self.state_full))
                query = """INSERT INTO census_tracts (census_tract_id, census_tract_code, 
                                                        state_id, county_id, county_name, state_abbr, state)
                                VALUES (%s, %s, %s, %s, %s, %s, %s) 
                                ON CONFLICT (census_tract_id) DO NOTHING
                                RETURNING census_tract_id 
                                ;"""
                self.cursor.execute(query, (census_tract_id, census_tract_code,
                                                state_id, county_id, county_name, self.state_abbr, self.state_full))
                self.connection.commit()
            print('finished inserting into census_blocks and census_tracts')
        else:
            print('census_blocks and census_tracts already complete')

    def process_raw_data(self):
        self.process_appendix()
        self.process_templates()
        self.process_summary()
        self.process_census_blocks()

    @staticmethod
    def read_summary_file(file, names):
        """
            Read summary estimates/margins file and return a massaged DataFrame
            ready for data extraction.
            """
        print(file)
        print(names)
        df = ACSCensusTract.read_from_csv(file, names=names)
        df = df.rename(columns={'SEQUENCE': 'seq', 'LOGRECNO': 'census_block_code'})
        print('df:', df.columns)
        print(df)
        return df


    @staticmethod
    def read_from_csv(file, names):
        """
            Customized call to pandas.read_csv for reading header-less summary files.
            """
        name_set = set()
        names_without_duplicates = []
        for name in names:
            if name not in name_set:
                names_without_duplicates.append(name)
                name_set.add(name)
            else:
                index = 1
                new_name = None
                while new_name is None:
                    if '_'.join([name, str(index)]) in name_set:
                        index += 1
                    else:
                        new_name = '_'.join([name, str(index)])
                names_without_duplicates.append(new_name)
                name_set.add(new_name)
        return pd.read_csv(file, encoding='ISO-8859-1', names=names_without_duplicates,
                           header=None, na_values=['.', -1], dtype=str)


    def label_variables_by_table(self, table_id, variable_names, verbose=True):
        column_name_to_variable_id = {}
        for index, variable_name in enumerate(variable_names):
            query = """INSERT INTO survey_variable (variable_id, table_id, title, year)
                                    VALUES (%s, %s, %s, %s) 
                                    ON CONFLICT (variable_id) DO NOTHING
                                    RETURNING variable_id 
                                    ;"""
            variable_id = '_'.join([table_id, str(index + 1)])
            self.cursor.execute(query, (variable_id, table_id, variable_name, self.year))
            column_name_to_variable_id[variable_name] = variable_id
            self.connection.commit()
        if verbose:
            print('finished inserting into survey_variable')
        return column_name_to_variable_id


    def insert_into_variable_by_neighborhood(self, merged_df, variable_ids):
        for index, row in merged_df.iterrows():
            census_block_code = row['census_block_code']
            for variable_id in variable_ids:
                estimate = row[variable_id + '_Estimate']
                margin_of_error = row[variable_id + '_Margin']
                query = """INSERT INTO variable_by_block (variable_id, census_block_code, estimate, margin_of_error, year)
                                    VALUES (%s, %s, %s, %s, %s) 
                                    ON CONFLICT (variable_id, census_block_code) DO NOTHING
                                    RETURNING variable_id
                                    ;"""
                self.cursor.execute(query, (variable_id, census_block_code, estimate, margin_of_error, self.year))
            self.connection.commit()
        return True


    def build_table_by_id(self, table_id, verbose=True):
        if verbose:
            print('table_id:', table_id)
        seq = self.appx_A['seq'][self.appx_A['name'] == table_id].values[0]
        start_pos = self.appx_A['start'][self.appx_A['name'] == table_id].values[0]
        end_pos = self.appx_A['end'][self.appx_A['name'] == table_id].values[0]
        template = self.templates[seq]
        try:
            efile = self.efiles[seq]
            edf = self.read_summary_file(os.path.join(self.data_folder, self.summary_foldername, efile), names=template)
        except OSError as e:
            self.stderr_print(f'Estimates file {efile} error for {table_id}')
            self.stderr_print(f'{e}')
            sys.exit()

        try:
            mfile = self.mfiles[seq]
            mdf = self.read_summary_file(os.path.join(self.data_folder, self.summary_foldername, mfile), names=template)
        except OSError as e:
            self.stderr_print(f'Margins file {mfile} error for {table_id}')
            self.stderr_print(f'{e}')
            sys.exit()

        variable_column_posts = list(range(start_pos - 1, end_pos))
        variable_names = edf.columns[variable_column_posts]
        column_name_to_variable_id = self.label_variables_by_table(table_id, variable_names, verbose)

        census_block_code_column_index = list(edf.columns).index('census_block_code')
        use_col_nums = [census_block_code_column_index] + variable_column_posts
        edf = edf.iloc[:, use_col_nums]
        mdf = mdf.iloc[:, use_col_nums]
        # Prepend E/M to column names for Estimates/Margins-of-Error
        renamed_edf_columns = []
        for col in edf.columns:
            if col == 'census_block_code':
                renamed_edf_columns.append(col)
            else:
                renamed_edf_columns.append(column_name_to_variable_id[col] + '_Estimate')
        renamed_mdf_columns = []
        for col in mdf.columns:
            if col == 'census_block_code':
                renamed_mdf_columns.append(col)
            else:
                renamed_mdf_columns.append(column_name_to_variable_id[col] + '_Margin')
        edf.columns = renamed_edf_columns
        mdf.columns = renamed_mdf_columns
        # Join Estimates and Margins-of-Error
        merged_df = edf.merge(mdf, on='census_block_code')
        if verbose:
            print('got merged_df for', table_id, '\t dataframe shape:', merged_df.shape)
        self.insert_into_variable_by_neighborhood(merged_df, column_name_to_variable_id.values())

    def insert_appx_A_into_survey_table(self):
        for index, row in self.appx_A.iterrows():
            table_id = row['name']
            table_title = row['title']
            table_restriction = row['restr']
            table_topics = row['topics']
            table_universe = row['universe']
            table_variable_count = row['end'] - row['start'] + 1
            query = """INSERT INTO survey_table (table_id, title, restriction, topics, universe, table_variable_count, year)
                                VALUES (%s, %s, %s, %s, %s, %s, %s) 
                                ON CONFLICT (table_id) DO NOTHING
                                RETURNING table_id
                                ;"""
            self.cursor.execute(query, (
            table_id, table_title, table_restriction, table_topics, table_universe, table_variable_count, self.year))
            self.connection.commit()
        return False

    def loop_over_table_ids(self, verbose=True):
        self.insert_appx_A_into_survey_table()
        print('finished inserting into survey_table')
        for index, row in tqdm(self.appx_A.iterrows(), total=len(self.appx_A)):
            table_id = row['name']
            self.build_table_by_id(table_id, verbose)


# todo compile a list of table_ids to loop through


if __name__ == '__main__':
    acs = ACSCensusTract('2018', 'NY')
    list_of_table_ids = ['B01001']  # can loop through this table
    list_of_us_states = acs.uss.all_states(abbr=True)  # can loop through this list
    for state_abbr in list_of_us_states:
        print(f'== {state_abbr} ==')
        acs.set_new_state(state_abbr)
        acs.download_raw_data_by_state()
        acs.process_raw_data()
    #table_id = 'B01001'
    #acs.build_table_by_id(table_id)
    # acs.loop_over_table_ids(verbose=False)
