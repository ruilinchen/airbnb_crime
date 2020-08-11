"""
build_acs_database.py

this program includes a number of classes to help users build a database
that stores American Community Survey (ACS) 5-year Estimates and Margins-of-Error
by variable_id and spatial_unit_id
(for now, the program collects variables measured by census tracts and
census_block_groups)
# todo include other spatial units such as zip_code, county_id

Class Components:
    - PrintError: contains a function "stderr_print()" that can be used in
                    a try-except block to catch exceptions and print error messages
    - FileStructure: stores file and/or folder paths and a psql cursor
    - CensusBlock2Tract: insert the correspondence between census_block_id,
                        census_block_group_id and census_tract_id into psql
    - Downloader: download different files needed for insertion
    - ACSTableStructure: construct table_id and variable_id and insert their relationship into database
    - VariablesInSummaryFile: find the relationship between ACS variables and different summary_files,
                            and save this information in a dictionary
    - SummaryFilesByID(FileStructure): build a directory that helps locate summary_file (estimate or margin) by their id
    - ACSInsertion: insert ACS variables by state measured at the spatial unit of your choice

Dependencies:
    - third-party packages:
        - psycopg2: connect python to postgresql
    - local modules:
        - airbnb_disorder_analytics.config
            - db_config: connect python to a specific postgresql database
            - us_states: handling US state names

Includes test case for:
    - the Downloader class
    - the ACSTableStructure class
    - the CensusBlock2Tract class
    - the ACSInsertion class

ruilin
08/11/2020
"""

# system import
import os
import sys
import requests
import zipfile
import io
import pandas as pd
from tqdm import tqdm
# third-party import
import psycopg2
# local import
from airbnb_disorder_analytics.config.db_config import DBInfo
from airbnb_disorder_analytics.config.us_states import USStates

__all__ = ['ACSInsertion', 'Downloader', 'ACSTableStructure']


class FileStructure:
    """
    stores file and/or folder paths and a psql cursor
    """
    def __init__(self, year, state_abbreviation):
        """

        :param year: int: a specific year of the ACS5 survey data to extract (latest: 2018)
        :param state_abbreviation: str: a specific US state to extract,
                                        in the abbreviated form that includes two characters
        :return: None
        """
        self.uss = USStates()
        self.root_path = '/home/rchen/Documents/github/airbnb_crime/airbnb_disorder_analytics/psql'
        self.data_folder = os.path.join(self.root_path, 'acs5_data')  # where all the acs5-related data is stored
        if not os.path.isdir(self.data_folder):
            os.mkdir(self.data_folder)
        self.year = int(year)
        self.state_abbr = state_abbreviation
        self.state_full = self.uss.abbr2name(state_abbreviation, ' ').title().replace(' ', '')
        self.summary_foldername = f'{self.state_full}_Tracts_Block_Groups_Only'  # where the summary zip file unzips
        self.templates_foldername = f'{self.year}_5yr_Summary_FileTemplates'  # where the template zip file unzips
        self.appendix_filename = f'ACS_{self.year}_SF_5YR_Appendices.xls'
        # where files related to the correspondence between census block group and
        # census tract are stored
        self.census_block_foldername = 'census_blocks'
        self.gfilename = f'g{self.year}5{self.state_abbr.lower()}.csv'  # the geo-info file inside the summary folder
        # where the ids of our interested ACS5 variables are stored
        self.key_acs5_variables_filepath = '../../analytics/key_acs5_variables.csv'
        # has detailed information on different ACS tables
        self.table_desc_filename = f'ACS5_{self.year}_table_descriptions.csv'
        # psql connection
        self.connection = psycopg2.connect(DBInfo.acs_config)
        self.cursor = self.connection.cursor()


class CensusBlock2Tract(FileStructure):
    """
    insert the correspondence between census_block_id,
    census_block_group_id and census_tract_id into psql
    """
    def __init__(self, year, state_abbreviation):
        """

        :param year: int: a specific year of the ACS5 survey data to extract (latest: 2018)
        :param state_abbreviation: str: a specific US state to extract,
                                        in the abbreviated form that includes two characters
        :return: None
        """
        super().__init__(year, state_abbreviation)

    def count_census_blocks_by_state(self):
        """

        :return: the number of census_blocks available in the current database for a given state
        note: can be used to determine if the database is complete or needs to be updated
        """
        query = """SELECT COUNT(*)
                    FROM census_blocks
                    WHERE state_abbr = '{}'
                    ;
                    """.format(self.state_abbr)
        self.cursor.execute(query)
        census_block_count = self.cursor.fetchone()[0]
        return census_block_count

    def insert_census_blocks_into_psql(self, verbose=True):
        """
        insert the relationship between census_block_id, census_block_group_id and census_tract_id
        into database

        :param verbose: Boolean --> whether to print output as the program goes
        :return: None
        """
        block_df = pd.read_csv(os.path.join(self.data_folder, self.census_block_foldername, self.state_abbr+'.csv'),
                               encoding='ISO-8859-1')  # the raw data
        existing_census_block_count = self.count_census_blocks_by_state()
        # check if the database already has complete records of this state
        # only insert when it is incomplete
        if block_df.shape[0] > existing_census_block_count:
            print('start inserting into census_blocks')
            for index, row in tqdm(block_df.iterrows(), total=len(block_df)):
                # state_fips: two digits with left paddings of zero: 01 for Alabama
                state_id = str(row['state']).zfill(2)
                county_id = str(row['county']).zfill(2)  # county_fips: two digits with left paddings of zero
                county_name = row['cnamelong']
                # [census_tract/census_block]_id is the long format that includes state_id and county_id as well
                # [census_tract/census_block]_code is the short format that does not have state_id and county_id
                census_tract_id = str(row['tractcode']).zfill(11)
                census_block_id = str(row['blockcode']).zfill(15)
                census_tract_code = str(row['tract']).zfill(5)
                census_block_code = str(row['block']).zfill(4)
                # insert into two tables: census_blocks and census_tracts
                # these two tables can be linked by "census_tract_id"
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


class PrintError:
    @staticmethod
    def stderr_print(*args, **kwargs):
        """
        print out error messages
        designed to use in try and except to catch exceptions
        """
        print(*args, **kwargs, file=sys.stderr, flush=True)


class Downloader(FileStructure):
    """
    download different files needed for insertion
    """
    def __init__(self, year, state_abbreviation):
        """

        :param year: int: a specific year of the ACS5 survey data to extract (latest: 2018)
        :param state_abbreviation: str: a specific US state to extract,
                                        in the abbreviated form that includes two characters
        :return: None
        """
        super().__init__(year, state_abbreviation)
        # get the state name by state abbreviation in format: New York -> NewYork
        self.acs_base_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/{}'.format(self.year)
        self.census_block_base_url = r'https://transition.fcc.gov/form477/Geo/CensusBlockData/CSVFiles'
        # https://transition.fcc.gov/form477/Geo/CensusBlockData/CSVFiles/District%20of%20Columbia.zip

    @staticmethod
    def download_and_unzip_zip_file(zip_url, path_to_unzipped_folder):
        """
        download zip_file from online and unzip the files inside into
        the designated folder

        :param zip_url: str
        :param path_to_unzipped_folder: str
        :return: None
        """
        try:
            print(f'Requesting zip file {zip_url}')
            response = requests.get(zip_url, timeout=3.333)
            response.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall(path_to_unzipped_folder)
            print('downloaded zip file and unzipped the data to folder', path_to_unzipped_folder)
        except requests.exceptions.RequestException as e:
            PrintError.stderr_print(f'Error: Download from {zip_url} failed. Reason: {e}')

    @staticmethod
    def download_file(url, path_to_file):
        """
        download file from online and save it to the designated path

        :param url: str
        :param path_to_file: str
        :return: None
        """
        try:
            response = requests.get(url, timeout=3.333)
            response.raise_for_status()
            with open(path_to_file, 'wb') as f:
                f.write(response.content)
                print('downloaded excel file', path_to_file)
        except requests.exceptions.RequestException as e:
            PrintError.stderr_print(f'Error: Download from {url} failed. Reason: {e}')

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


class ACSTableStructure(FileStructure):
    """
    construct table_id and variable_id and insert their relationship into database
    """
    def __init__(self, year, state_abbreviation):
        """

        :param year: int: a specific year of the ACS5 survey data to extract (latest: 2018)
        :param state_abbreviation: str: a specific US state to extract,
                                        in the abbreviated form that includes two characters
        :return: None
        """
        super().__init__(year, state_abbreviation)
        self.table_structure_df = None
        self.table_structure_df = self._build_acs_tables()
        # get survey table ids and their descriptions, save them to csv
        if not os.path.isfile(os.path.join(self.data_folder, self.table_desc_filename)):
            table_df = self.table_structure_df.filter(['name', 'title'], axis=1)
            table_df.to_csv(os.path.join(self.data_folder, self.table_desc_filename), index=False)

    def _build_acs_tables(self):
        """
        read sheet: appendix a of the appendix file to learn about the number of variables associated with
        each acs table (the start_end column), and create variable_id as table_id_{variable_index}.

        :return: a pandas dataframe
        """
        with open(os.path.join(self.data_folder, self.appendix_filename), 'rb') as r:
            appx_df = pd.read_excel(r, converters={'Summary File Sequence Number': str})
            appx_df.columns = ['name', 'title', 'restr', 'seq', 'start_end', 'topics', 'universe']
            # appx_A = appx_A[appx_A['restr'].str.contains('No Blockgroups') == False]
            try:
                appx_df[['start', 'end']] = appx_df['start_end'].str.split('-', 1, expand=True)
                appx_df['start'] = pd.to_numeric(appx_df['start'])
                appx_df['end'] = pd.to_numeric(appx_df['end'])
                return appx_df
            except ValueError as e:
                PrintError.stderr_print(f'{e}')
                PrintError.stderr_print(
                    f'File {os.path.join(self.data_folder, self.appendix_filename)} is corrupt or has invalid format')
                raise SystemExit(f'Exiting {__file__}')

    def insert_table_structure_into_psql(self, verbose):
        """
        insert the constructed table_structure_df with detailed information on the
        properties of the table as well as the associated variables

        :param verbose: boolean -> whether to print outputs as the program goes
        :return: None
        """
        for index, row in self.table_structure_df.iterrows():
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
            if verbose:
                print(f'inserted {self.cursor.fetchone()} into psql')
            self.connection.commit()
        print('finished inserting table structure into psql')


class VariablesInSummaryFile(ACSTableStructure):
    """
    find the relationship between ACS variables and different summary_files (the list of ACS
    variables we can find in different summary files), and save this information in a dictionary

    this relationship can be found in the summary_templates, which provide the header for the
    corresponding summary files. The index of the column on each variable included in this header,
    is also the index of the column where the estimate and margin-of-error of this variable is stored
    in the E-file and the M-file.
    """
    def __init__(self, year, state_abbreviation, verbose=False):
        super().__init__(year, state_abbreviation)
        self.variables_in_summary = {}
        self._build_variables2summary_directory(verbose)

    def _build_variables2summary_directory(self, verbose=False):
        for filename in os.listdir(os.path.join(self.data_folder, self.templates_foldername)):
            if 'seq' in filename.lower():
                # Generate 4-digit sequence number string
                file_index = filename.lower().index('seq')
                # Drop 'seq' and separate sequence number from file extension
                s = filename.lower()[file_index + 3:].split('.')[0]
                # Generate number string
                key = s.zfill(4)  # the id used to locate summary files; the same id is used to name summary_file
            elif 'geo' in filename.lower():
                key = 'geo'
            else:
                # skip directories or other files
                continue
            template_df = pd.read_excel(os.path.join(self.data_folder, self.templates_foldername, filename))
            # Extract column names from data row 0
            self.variables_in_summary[key] = template_df.loc[0].tolist()
        if verbose:
            print('finished processing templates folder and constructed a directory of the summary file:',
                  self.templates_foldername)


class SummaryFilesByID(FileStructure):
    """
    build a directory that helps locate summary_file (estimate or margin) by their id
    can be used to connect efile, mfile with the summary_template.
    """
    def __init__(self, year, state_abbreviation):
        super().__init__(year, state_abbreviation)
        self.efile_by_id = {}
        self.mfile_by_id = {}
        self._build_id2file_directory()

    def _build_id2file_directory(self):
        """
        iterate over the summary folder, extract the eighth to twelfth digit of the file name
        as summary_file_id and use this as the key for the directory -> value is the filename

        creates two dictionaries, one for estimate files, and another for margin-of-error files
        :return: None
        """
        e = [f for f in os.listdir(os.path.join(self.data_folder, self.summary_foldername)) if f.startswith('e')]
        # Pull sequence number from file name positions 8-11; use as dict key
        self.efile_by_id = {f[8:12]: f for f in e}
        # Get Margin-of-Error file names
        m = [f for f in os.listdir(os.path.join(self.data_folder, self.summary_foldername)) if f.startswith('m')]
        # Pull sequence number from file name positions 8-11; use as dict key
        self.mfile_by_id = {f[8:12]: f for f in m}


class ACSInsertion(ACSTableStructure):
    """
    insert ACS data by variable_id
    """
    def __init__(self, year, state_abbreviation, verbose=True):
        super().__init__(year, state_abbreviation)
        self.variables_in_summary = {}
        self.efile_by_id = {}
        self.mfile_by_id = {}
        self.summary_level_for_census_block = '150'
        self.summary_level_for_census_tract = '140'

        vsf = VariablesInSummaryFile(year, state_abbreviation, verbose)  # build the summary_to_variable_list directory
        self.variables_in_summary = vsf.variables_in_summary

        sf = SummaryFilesByID(year, state_abbreviation)  # build the summary_id_to_filename directory
        self.efile_by_id = sf.efile_by_id
        self.mfile_by_id = sf.mfile_by_id

    def set_new_state(self, state_abbreviation, verbose=False):
        self.__init__(self.year, state_abbreviation, verbose)

    @staticmethod
    def read_summary_file(file, column_names):
        """
            Read summary estimates/margins file and return a massaged DataFrame
            ready for data extraction.
            """
        summary_df = ACSInsertion.read_from_csv(file, column_names=column_names)
        summary_df = summary_df.rename(columns={'SEQUENCE': 'seq', 'LOGRECNO': 'logical_record_number'})
        return summary_df

    @staticmethod
    def read_from_csv(file, column_names):
        """
        customized call to pandas.read_csv for reading header-less summary files.
        replace duplicates in column_names with column_name_{index}
        """
        name_set = set()
        names_without_duplicates = []
        for name in column_names:
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

    def insert_variable_into_psql(self, variable_id, census_block=False, census_tract=False, verbose=True):
        """
        insert ACS data into database by providing table_id, variable_id

        :param variable_id: str
        :param census_block: boolean -> whether the neighborhood is defined as census block
        :param census_tract: boolean -> whether the neighborhood is defined as census tract
        :param verbose: boolean -> whether to print output as the program goes
        :return: None
        """
        merged_df = self._merge_summary_files_by_id(variable_id, census_block, census_tract, verbose)
        for index, row in merged_df.iterrows():
            if census_block:
                query = """INSERT INTO variable_by_block (variable_id, census_tract_id, census_block_group, estimate, 
                                                            margin_of_error, year)
                                    VALUES (%s, %s, %s, %s, %s, %s) 
                                    ON CONFLICT (variable_id, census_tract_id, census_block_group) DO NOTHING
                                    RETURNING variable_id
                                    ;"""
                self.cursor.execute(query, (variable_id, row['census_tract_id'], row['census_block_group'],
                                            row[variable_id + '_Estimate'], row[variable_id + '_Margin'], self.year))
            if census_tract:
                query = """INSERT INTO variable_by_tract (variable_id, census_tract_id, estimate, 
                                                            margin_of_error, year)
                                    VALUES (%s, %s, %s, %s, %s) 
                                    ON CONFLICT (variable_id, census_tract_id) DO NOTHING
                                    RETURNING variable_id
                                    ;"""
                self.cursor.execute(query, (variable_id, row['census_tract_id'],
                                            row[variable_id + '_Estimate'], row[variable_id + '_Margin'], self.year))
            self.connection.commit()

    def _get_summary_column_index_by_variable_id(self, variable_id):
        """
        find the index of the column that records variable estimates and/or margins-of-error
        in the corresponding summary file

        :param variable_id: str
        :return: int
        """
        table_id = self.get_table_id_from_variable_id(variable_id)
        seq = self.table_structure_df['seq'][self.table_structure_df['name'] == table_id].values[0]
        list_of_variables_in_summary = self.variables_in_summary[seq]
        query = """SELECT variable_id, title
                                        FROM survey_variable
                                        WHERE table_id = '{}'
                                        AND variable_id = '{}'
                                        ;
                                        """.format(table_id, variable_id)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        target_variable_name = result[1]
        target_variable_column_index = list_of_variables_in_summary.index(target_variable_name)
        return target_variable_column_index

    def _merge_summary_files_by_id(self, variable_id, census_block=False, census_tract=False, verbose=True):
        """
        merge edf, mdf and gdf to get a complete table of variable_id that has both estimates, margins-of-error
        by neighborhood

        :param variable_id: str
        :param census_block: boolean -> whether the neighborhood is defined as census block
        :param census_tract: boolean -> whether the neighborhood is defined as census tract
        :param verbose: boolean -> whether to print output as the program goes
        :return: a pandas dataframe
        """
        table_id = self.get_table_id_from_variable_id(variable_id)
        seq = self.table_structure_df['seq'][self.table_structure_df['name'] == table_id].values[0]
        list_of_variables_in_summary = self.variables_in_summary[seq]
        column_index_of_variable_in_summary = self._get_summary_column_index_by_variable_id(variable_id)
        # read gdf, rename the two geo-columns and extract census_tract_id and/or census_block_id
        # from geographic_identifier
        gdf = self.read_from_csv(os.path.join(self.data_folder, self.summary_foldername, self.gfilename),
                                 column_names=self.variables_in_summary['geo'])
        if census_block:
            gdf = gdf[['Logical Record Number', 'Geographic Identifier']][gdf['Summary Level']
                                                                          == self.summary_level_for_census_block]
            gdf.columns = ['logical_record_number', 'geographic_identifier']
            gdf['census_tract_id'] = gdf['geographic_identifier'].str.split('US').str[1].str[:-1]
            gdf['census_block_group'] = gdf['geographic_identifier'].str.split('US').str[1].str[-1]

        if census_tract:
            gdf = gdf[['Logical Record Number', 'Geographic Identifier']][gdf['Summary Level'] ==
                                                                          self.summary_level_for_census_tract]
            gdf.columns = ['logical_record_number', 'geographic_identifier']
            gdf['census_tract_id'] = gdf['geographic_identifier'].str.split('US').str[1]

        # add two more columns on census_tract_id and census_block_group

        # read estimates and margins-of-error
        efile = self.efile_by_id[seq]
        edf = self.read_summary_file(os.path.join(self.data_folder, self.summary_foldername, efile),
                                     column_names=list_of_variables_in_summary)
        mfile = self.mfile_by_id[seq]
        mdf = self.read_summary_file(os.path.join(self.data_folder, self.summary_foldername, mfile),
                                     column_names=list_of_variables_in_summary)
        # keep only the geo-column and the column of the target_variable_id and rename the columns by
        # adding a postfix of '_Estimate' to edf variable column and that of '_Margin' to mdf variable column
        geo_code_column_index = list(edf.columns).index('logical_record_number')
        use_col_nums = [geo_code_column_index, column_index_of_variable_in_summary]
        edf = edf.iloc[:, use_col_nums]
        mdf = mdf.iloc[:, use_col_nums]
        edf.columns = ['logical_record_number', variable_id+'_Estimate']
        mdf.columns = ['logical_record_number', variable_id + '_Margin']

        # merge edf, mdf and gdf
        e_m_df = edf.merge(mdf, on='logical_record_number')
        merged_df = pd.merge(e_m_df, gdf, on='logical_record_number', how='inner')
        if verbose:
            print('got merged_df for', table_id, '\t dataframe shape:', merged_df.shape)
        return merged_df

    @staticmethod
    def get_table_id_from_variable_id(variable_id):
        """

        :param variable_id: str
        :return: str
        """
        return '_'.join(variable_id.split('_')[:-1])

    def get_variable_restriction_from_variable_id(self, variable_id):
        """
        get the restriction property of the variable

        :param variable_id: str
        :return: str
        """
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
        """
        get the list of ACS5 variables of interest provided by the user

        :return: a list of variable_ids
        """
        variable_df = pd.read_csv(os.path.join(self.data_folder, self.key_acs5_variables_filepath))
        return list(variable_df['variable_id'])

if __name__ == '__main__':
    """
    d = Downloader(2018, 'NY')   # test the functioning of the Downloader class
    d.download_raw_data_by_state()
    a = ACSTableStructure(2018, 'NY')  # test the functioning of the ACSTableStructure class
    a.insert_table_structure_into_psql(verbose=False)
    cbt = CensusBlock2Tract(2018, 'NY')
    cbt.insert_census_blocks_into_psql(verbose=False)  # test the functioning of the CensusBlock2Tract class
    """
    acs = ACSInsertion(2018, 'NY')   # test the functioning of the ACSInsertion class
    list_of_us_states = acs.uss.all_states()  # get a list of all US states in their abbreviations
    df = acs.get_key_acs5_variables()  # get a list of ACS5 variables to insert
    key_variable_ids = acs.get_key_acs5_variables()
    for key_variable_id in tqdm(key_variable_ids,  total=len(key_variable_ids)):
        print('== a new round: inserting data related to', key_variable_id)
        print('variable property:', acs.get_variable_restriction_from_variable_id(key_variable_id))
        for state_abbr in tqdm(list_of_us_states, total=len(list_of_us_states)):
            acs.set_new_state(state_abbr)
            acs.insert_variable_into_psql(key_variable_id, census_tract=True, verbose=False)
