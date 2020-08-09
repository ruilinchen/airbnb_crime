'''
build_acs_database.py

this program builds a database for American Community Survey 5-year Estimates by Census Tracts

ruilin
08/09/2020
'''

# system import
from airbnb_disorder_analytics.config.us_states import USStates


def download_raw_data(year, state_abbr):
    """
    download raw data of ACS 5-year estimates for a certain state and year from Census.gov

    :param year: int or str
    :param state_abbr: str
    :return: True
    """

    state_name = USStates.abbr2name(state_abbr, '')  # get the state name by state abbreviation in such a format: Florida -> Florida; New York -> NewYork
    acs_base_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/{}'.format(year)

    # https://www2.census.gov/programs-surveys/acs/summary_file/2018/data/5_year_by_state/DistrictOfColumbia_All_Geographies_Not_Tracts_Block_Groups.zip
    summary_file_url = '/'.join(
        [acs_base_url, 'data/5_year_by_state/{state}_Tracts_Block_Groups_Only.zip'.format(state=state_name)])
    appendix_file_url = '/'.join(
        [acs_base_url, 'documentation/tech_docs/ACS_{year}_SF_5YR_Appendices.xls'.format(year=year)])
    templates_file_url = '/'.join([acs_base_url, '/data/_5yr_Summary_FileTemplates.zip'])

    urls = [summary_file_url, appendix_file_url, templates_file_url]
    # Download files, as necessary
    for url in urls:
        basename, filename = os.path.split(url)
        if not os.path.exists(filename):
            print(f'Requesting file {url}')
            response = request_file(url)
            if response:
                try:
                    with open(pathname, 'wb') as w:
                        w.write(response.content)
                        print(f'File {pathname} downloaded successfully')
                except OSError as e:
                    stderr_print(f'Error {e}: File write on {pathname} failed')
