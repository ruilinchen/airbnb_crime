"""
build_acs_database.py

this program builds a database for American Community Survey 5-year Estimates by Census Tracts

ruilin
08/09/2020
"""

# system import
import os
import sys
import requests
import zipfile
import io
import pandas as pd
# third-party import
import psycopg2
from tqdm import tqdm
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.config.us_states import USStates

__all__ = ['ACSCensusTract']


class ACSCensusTract:
    def __init__(self, year, state_abbreviation):
        self.data_folder = 'acs5_data'
        if not os.path.isdir(self.data_folder):
            os.mkdir(self.data_folder)
        self.year = year
        self.state_abbr = state_abbreviation
        self.uss = USStates()
        # get the state name by state abbreviation in format: New York -> NewYork
        self.state_full = self.uss.abbr2name(state_abbreviation, ' ').title().replace(' ', '')
        self.acs_base_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/{}'.format(self.year)
        self.summary_foldername = f'{self.state_full}_Tracts_Block_Groups_Only'
        self.templates_foldername = f'{self.year}_5yr_Summary_FileTemplates'
        self.geotemplate_filename = '2018_SFGeoFileTemplate.xlsx'
        self.census_block_foldername = f'census_blocks'
        self.appendix_filename = f'ACS_{self.year}_SF_5YR_Appendices.xls'
        self.table_desc_filename = f'ACS5_{self.year}_table_descriptions.csv'
        self.census_block_base_url = r'https://transition.fcc.gov/form477/Geo/CensusBlockData/CSVFiles'
        # https://transition.fcc.gov/form477/Geo/CensusBlockData/CSVFiles/District%20of%20Columbia.zip
        self.templates = {}
        self.appx_df = None
        self.efiles = {}
        self.mfiles = {}
        self.gfilename = f'g20185{self.state_abbr.lower()}.csv'
        self.connection = psycopg2.connect(DBInfo.acs_config)
        self.cursor = self.connection.cursor()
        self.key_acs5_variables_filepath = '../analytics/key_acs5_variables.csv'
        self.summary_level_for_census_block = '150'
        self.summary_level_for_census_tract = '140'

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

    def set_new_state(self, state_abbreviation):
        self.state_abbr = state_abbreviation
        self.state_full = self.uss.abbr2name(state_abbreviation, ' ').title().replace(' ', '')
        self.summary_foldername = f'{self.state_full}_Tracts_Block_Groups_Only'
        self.gfilename = f'g20185{self.state_abbr.lower()}.csv'
        self._process_summary(verbose=False)

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
        # download and unzip census block zip file
        if not os.path.isfile(os.path.join(self.data_folder, self.census_block_foldername, self.state_abbr+'.csv')):
            self.download_and_unzip_zip_file(census_block_zip_url,
                                             os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}'))
            # reorganize census_block_by_state files and store all of them in a folder called "census_blocks"
            # by first moving the file into the new folder and then rename the file to "{state_abbr}.csv"
            if not os.path.isdir(os.path.join(self.data_folder, self.census_block_foldername)):
                # create a folder called "census_blocks"
                os.mkdir(os.path.join(self.data_folder, self.census_block_foldername))
            census_block_filename = None
            for file in os.listdir(os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}')):
                if '.csv' in file:
                    census_block_filename = file
                # move the file to "census_blocks" and rename it to "{state_abbr}.csv"
                os.rename(os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}', census_block_filename),
                          os.path.join(self.data_folder, self.census_block_foldername, self.state_abbr+'.csv'))
                # remove the "census_block_{state}" folder
                os.rmdir(os.path.join(self.data_folder, f'census_blocks_{self.state_abbr}'))

        # Download Excel files
        if not os.path.isfile(os.path.join(self.data_folder, self.appendix_filename)):
            self.download_file(appendix_file_url, os.path.join(self.data_folder, self.appendix_filename))

    def _process_appendix(self, verbose=True):
        with open(os.path.join(self.data_folder, self.appendix_filename), 'rb') as r:
            appx_df = pd.read_excel(r, converters={'Summary File Sequence Number': str})
            appx_df.columns = ['name', 'title', 'restr', 'seq', 'start_end', 'topics', 'universe']
            # appx_A = appx_A[appx_A['restr'].str.contains('No Blockgroups') == False]
            try:
                appx_df[['start', 'end']] = appx_df['start_end'].str.split('-', 1, expand=True)
                appx_df['start'] = pd.to_numeric(appx_df['start'])
                appx_df['end'] = pd.to_numeric(appx_df['end'])
                self.appx_df = appx_df
                if verbose:
                    print('finished processing appendix file:', self.appendix_filename)
            except ValueError as e:
                self.stderr_print(f'{e}')
                self.stderr_print(
                    f'File {os.path.join(self.data_folder, self.appendix_filename)} is corrupt or has invalid format')
                raise SystemExit(f'Exiting {__file__}')
        # print('read appx_A into class')
        if not os.path.isfile(os.path.join(self.data_folder, self.table_desc_filename)):
            # get survey table ids and their descriptions, save them to csv
            table_df = self.appx_df.filter(['name', 'title'], axis=1)
            table_df.to_csv(os.path.join(self.data_folder, self.table_desc_filename), index=False)
            if verbose:
                print('saved table descriptions to csv:', self.table_desc_filename)

    def _process_templates(self, verbose=True):
        self.templates = {}
        for filename in os.listdir(os.path.join(self.data_folder, self.templates_foldername)):
            if 'seq' in filename.lower():
                # Generate 4-digit sequence number string
                file_index = filename.lower().index('seq')
                # Drop 'seq' and separate sequence number from file extension
                s = filename.lower()[file_index + 3:].split('.')[0]
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
        if verbose:
            print('finished processing templates folder:', self.templates_foldername)

    def _process_summary(self, verbose=True):
        e = [f for f in os.listdir(os.path.join(self.data_folder, self.summary_foldername)) if f.startswith('e')]
        # Pull sequence number from file name positions 8-11; use as dict key
        self.efiles = {f[8:12]: f for f in e}
        # Get Margin-of-Error file names
        m = [f for f in os.listdir(os.path.join(self.data_folder, self.summary_foldername)) if f.startswith('m')]
        # Pull sequence number from file name positions 8-11; use as dict key
        self.mfiles = {f[8:12]: f for f in m}
        if verbose:
            print('finished processing summary folder:', self.summary_foldername)

    def _count_census_blocks_by_state(self):
        query = """SELECT COUNT(*)
                    FROM census_blocks
                    WHERE state_abbr = '{}'
                    ;
                    """.format(self.state_abbr)
        self.cursor.execute(query)
        census_block_count = self.cursor.fetchone()[0]
        return census_block_count

    def _process_census_blocks(self, verbose=True):
        block_df = pd.read_csv(os.path.join(self.data_folder, self.census_block_foldername, self.state_abbr+'.csv'),
                               encoding='ISO-8859-1')
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
                query = """INSERT INTO census_blocks (census_block_id, census_block_code, census_tract_id, 
                                                        census_tract_code, state_id, county_id, county_name, 
                                                        state_abbr, state)
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
                self.cursor.execute(query, (census_tract_id, census_tract_code, state_id,
                                            county_id, county_name, self.state_abbr, self.state_full))
                self.connection.commit()
            if verbose:
                print('finished inserting into census_blocks and census_tracts')
        else:
            if verbose:
                print('census_blocks and census_tracts already complete')

    def process_raw_data(self, verbose=True):
        self._process_appendix(verbose)
        self._process_templates(verbose)
        self._process_summary(verbose)
        self._process_census_blocks(verbose)

    @staticmethod
    def read_summary_file(file, names):
        """
            Read summary estimates/margins file and return a massaged DataFrame
            ready for data extraction.
            """
        df = ACSCensusTract.read_from_csv(file, names=names)
        df = df.rename(columns={'SEQUENCE': 'seq', 'LOGRECNO': 'logical_record_number'})
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

    def insert_into_survey_variable_by_table(self, table_id, verbose=True):
        if verbose:
            print('table_id:', table_id)
        seq = self.appx_df['seq'][self.appx_df['name'] == table_id].values[0]
        start_pos = self.appx_df['start'][self.appx_df['name'] == table_id].values[0]
        end_pos = self.appx_df['end'][self.appx_df['name'] == table_id].values[0]
        template = self.templates[seq]
        efile = self.efiles[seq]
        try:
            edf = self.read_summary_file(os.path.join(self.data_folder, self.summary_foldername, efile), names=template)
        except OSError as e:
            self.stderr_print(f'Estimates file {efile} error for {table_id}')
            self.stderr_print(f'{e}')
            sys.exit()

        variable_column_posts = list(range(start_pos - 1, end_pos))
        variable_names = edf.columns[variable_column_posts]
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

    def _insert_into_variable_by_block(self, merged_df, variable_ids):
        for variable_id in variable_ids:
            for index, row in merged_df.iterrows():
                query = """INSERT INTO variable_by_block (variable_id, census_tract_id, census_block_group, estimate, 
                                                            margin_of_error, year)
                                    VALUES (%s, %s, %s, %s, %s, %s) 
                                    ON CONFLICT (variable_id, census_tract_id, census_block_group) DO NOTHING
                                    RETURNING variable_id
                                    ;"""
                self.cursor.execute(query, (variable_id, row['census_tract_id'], row['census_block_group'],
                                            row[variable_id + '_Estimate'], row[variable_id + '_Margin'], self.year))
            self.connection.commit()
        return True

    def _insert_into_variable_by_tract(self, merged_df, variable_ids):
        for variable_id in variable_ids:
            for index, row in merged_df.iterrows():
                query = """INSERT INTO variable_by_tract (variable_id, census_tract_id, estimate, 
                                                            margin_of_error, year)
                                    VALUES (%s, %s, %s, %s, %s) 
                                    ON CONFLICT (variable_id, census_tract_id) DO NOTHING
                                    RETURNING variable_id
                                    ;"""
                self.cursor.execute(query, (variable_id, row['census_tract_id'], row[variable_id + '_Estimate'],
                                            row[variable_id + '_Margin'], self.year))
            self.connection.commit()
        return True

    def _count_variable_by_block_by_state_and_id(self, variable_id):
        query = '''SELECT COUNT(variable_by_block.variable_id)
                    FROM variable_by_block, census_blocks
                    WHERE variable_by_block.variable_id = %s
                    AND variable_by_block.census_tract_id = census_blocks.census_tract_id
                    AND variable_by_block.census_block_group = census_blocks.census_block_group
                    AND census_blocks.state_abbr = '{}'
                    ;
                    '''.format(self.state_abbr)
        self.cursor.execute(query, (variable_id,))
        result = self.cursor.fetchone()
        return result[0]

    def insert_survey_variable_into_psql(self, table_id, variable_id=None, census_tract=False,
                                         census_block=False, verbose=True):
        if verbose:
            print('table_id:', table_id)
        seq = self.appx_df['seq'][self.appx_df['name'] == table_id].values[0]
        start_pos = self.appx_df['start'][self.appx_df['name'] == table_id].values[0]
        end_pos = self.appx_df['end'][self.appx_df['name'] == table_id].values[0]
        template = self.templates[seq]
        geo_template = self.templates['geo']
        # read geo-info
        try:
            gdf = self.read_from_csv(os.path.join(self.data_folder, self.summary_foldername, self.gfilename),
                                     names=geo_template)
            if census_block:
                gdf = gdf[['Logical Record Number', 'Geographic Identifier']][gdf['Summary Level']
                                                                              == self.summary_level_for_census_block]
            if census_tract:
                gdf = gdf[['Logical Record Number', 'Geographic Identifier']][gdf['Summary Level'] ==
                                                                              self.summary_level_for_census_tract]
            gdf.columns = ['logical_record_number', 'geographic_identifier']
            gdf['census_tract_id'] = gdf['geographic_identifier'].str.split('US').str[1].str[:-1]
            gdf['census_block_group'] = gdf['geographic_identifier'].str.split('US').str[1].str[-1]
        except OSError as e:
            self.stderr_print(f'Estimates file {self.gfilename} error')
            self.stderr_print(f'{e}')
            sys.exit()
        # read estimates
        efile = self.efiles[seq]
        try:
            edf = self.read_summary_file(os.path.join(self.data_folder, self.summary_foldername, efile), names=template)
        except OSError as e:
            self.stderr_print(f'Estimates file {efile} error for {table_id}')
            self.stderr_print(f'{e}')
            sys.exit()
        # read margins-of-error
        mfile = self.mfiles[seq]
        try:
            mdf = self.read_summary_file(os.path.join(self.data_folder, self.summary_foldername, mfile), names=template)
        except OSError as e:
            self.stderr_print(f'Margins file {mfile} error for {table_id}')
            self.stderr_print(f'{e}')
            sys.exit()

        if variable_id is None:
            variable_column_posts = list(range(start_pos - 1, end_pos))
            # get column_name_to_variable_id
            query = ('SELECT variable_id, title\n'
                     '                        FROM survey_variable\n'
                     '                        WHERE table_id = \'{}\'\n'
                     '                        ;\n'
                     '                        ').format(table_id)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            column_name_to_variable_id = {result[1]: result[0] for result in results}

            geo_code_column_index = list(edf.columns).index('logical_record_number')
            use_col_nums = [geo_code_column_index] + variable_column_posts
        else:
            # get column_name_to_variable_id
            query = """SELECT variable_id, title
                                    FROM survey_variable
                                    WHERE table_id = '{}'
                                    AND variable_id = '{}'
                                    ;
                                    """.format(table_id, variable_id)
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            column_name_to_variable_id = {result[1]: result[0]}
            target_variable_name = result[1]
            geo_code_column_index = list(edf.columns).index('logical_record_number')
            target_variable_column_index = list(edf.columns).index(target_variable_name)
            use_col_nums = [geo_code_column_index, target_variable_column_index]
        edf = edf.iloc[:, use_col_nums]
        mdf = mdf.iloc[:, use_col_nums]
        # Prepend E/M to column names for Estimates/Margins-of-Error
        renamed_edf_columns = []
        for col in edf.columns:
            if col == 'logical_record_number':
                renamed_edf_columns.append(col)
            else:
                renamed_edf_columns.append(column_name_to_variable_id[col] + '_Estimate')
        renamed_mdf_columns = []
        for col in mdf.columns:
            if col == 'logical_record_number':
                renamed_mdf_columns.append(col)
            else:
                renamed_mdf_columns.append(column_name_to_variable_id[col] + '_Margin')
        edf.columns = renamed_edf_columns
        mdf.columns = renamed_mdf_columns
        # Join Estimates and Margins-of-Error
        e_m_df = edf.merge(mdf, on='logical_record_number')
        # then merge with geo-info
        merged_df = pd.merge(e_m_df, gdf, on='logical_record_number', how='inner')
        if verbose:
            print('got merged_df for', table_id, '\t dataframe shape:', merged_df.shape)
        if len(set(merged_df[f'{variable_id}_Estimate'])) > 1:
            if census_block:
                self._insert_into_variable_by_block(merged_df, column_name_to_variable_id.values())
            if census_tract:
                self._insert_into_variable_by_tract(merged_df, column_name_to_variable_id.values())
            return True
        else:
            print('no meaningful data to insert')
            return False

    def insert_appx_df_into_survey_table(self):
        for index, row in self.appx_df.iterrows():
            table_id = row['name']
            table_title = row['title']
            table_restriction = row['restr']
            table_topics = row['topics']
            table_universe = row['universe']
            table_variable_count = row['end'] - row['start'] + 1
            query = ("INSERT INTO survey_table (table_id, title, restriction, topics, universe, \n"
                     "                                                table_variable_count, year)\n"
                     "                                VALUES (%s, %s, %s, %s, %s, %s, %s) \n"
                     "                                ON CONFLICT (table_id) DO NOTHING\n"
                     "                                RETURNING table_id\n"
                     "                                ;")
            self.cursor.execute(query, (table_id, table_title, table_restriction, table_topics,
                                        table_universe, table_variable_count, self.year))
            self.connection.commit()
        return False

    def loop_over_table_ids(self, verbose=True):
        self.insert_appx_df_into_survey_table()
        print('finished inserting into survey_table')
        for index, row in tqdm(self.appx_df.iterrows(), total=len(self.appx_df)):
            table_id = row['name']
            self.insert_survey_variable_into_psql(table_id, verbose)

    @staticmethod
    def get_table_id_from_variable_id(variable_id):
        return '_'.join(variable_id.split('_')[:-1])

    def get_variable_properties_from_variable_id(self, variable_id):
        query = """SELECT survey_table.restriction
                    FROM survey_variable, survey_table
                    WHERE survey_variable.variable_id = %s
                    AND survey_variable.table_id = survey_table.table_id
                    ;
                    """
        self.cursor.execute(query, (variable_id, ))
        result = self.cursor.fetchone()
        return result[0]

    def get_key_acs5_variables(self):
        df = pd.read_csv(self.key_acs5_variables_filepath)
        return list(df['variable_id'])


if __name__ == '__main__':
    acs = ACSCensusTract('2018', 'NY')
    list_of_table_ids = ['B01001']  # can loop through this table
    list_of_us_states = acs.uss.all_states(abbr=True)  # can loop through this list

    # for state_abbr in list_of_us_states:
    #     print(f'== {state_abbr} ==')
    #     acs.set_new_state(state_abbr)
    #     acs.download_raw_data_by_state()
    #     acs.process_raw_data()

    # table_id = 'B01001'
    # acs.build_table_by_id(table_id)
    # acs.loop_over_table_ids(verbose=False)

    # insert variables into database for query
    # acs.process_raw_data()
    # for index, row in tqdm(acs.appx_A.iterrows(), total=len(acs.appx_A)):
    #     table_id = row['name']
    #     acs.label_variables_by_table(table_id, verbose=False)

    # insert tables into database for query
    # acs.process_raw_data()
    # acs.insert_appx_A_into_survey_table()

    # insert all survey data related to key acs5 variables into database
    key_variable_ids = acs.get_key_acs5_variables()
    for key_variable_id in key_variable_ids:
        print('== a new round: inserting data related to', key_variable_id)
        target_table_id = acs.get_table_id_from_variable_id(key_variable_id)
        print('variable property:', acs.get_variable_properties_from_variable_id(key_variable_id))
        for state_abbr in tqdm(list_of_us_states, total=len(list_of_us_states)):
            acs.set_new_state(state_abbr)
            acs.process_raw_data(verbose=False)
            has_data_to_update = acs.insert_survey_variable_into_psql(target_table_id, variable_id=key_variable_id,
                                                                      census_tract=True, verbose=False)
            if not has_data_to_update:
                break
