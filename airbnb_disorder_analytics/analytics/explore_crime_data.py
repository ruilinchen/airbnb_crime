import os
import pandas as pd

class CrimeData:
    def __init__(self):
        self.data_folder = '/home/rchen/Documents/github/airbnb_crime/data/crime_data'
        self.code_folder = 'crime_code'
        self.crime_filename_by_city = {
            'austin': 'Austin_Crime_Reports.csv',
            'boston': 'BOS_crime_incident_from_2015.csv',
            'chicago': 'Chicago_Crimes_-_2019.csv',
            'dc': 'DC_Crime_Incidents_in_2019.csv',
            'la': 'LA_Crime_Data_from_2010_to_2019.csv',
            'nyc': 'NY_Complaint_Data_2019.csv',
            'seattle': 'Seattle_from_2008_to_2019.csv',
            'sf': 'SF_Police_Department_Incident_Reports__2018_to_Present.csv',
        } # not complete; some cities have more than one file

    def save_crime_codes_to_file(self, city, description_column, code_column=None):
        city_df = pd.read_csv(os.path.join(self.data_folder, self.crime_filename_by_city[city]))
        if code_column is not None:
            crime_dict = pd.Series(city_df[description_column].values, index=city_df[code_column]).to_dict()
            df = pd.DataFrame(columns=['offense_code', 'offense_description'])
            df['offense_code'] = list(crime_dict.keys())
            df['offense_description'] = list(crime_dict.values())
            df.to_csv(os.path.join(self.data_folder, self.code_folder, f'{city}_crime_code.csv'), index=False)
        else:
            df = pd.DataFrame(columns=['offense_description'])
            df['offense_description'] = list(set(city_df[description_column]))
            df.to_csv(os.path.join(self.data_folder, self.code_folder, f'{city}_crime_code.csv'), index=False)

    def explore_crime_data(self, city):
        df = pd.read_csv(os.path.join(self.data_folder, self.crime_filename_by_city[city]))
        print(df.columns)
        #print(df[['KY_CD', 'OFNS_DESC']][:10])
        #print(df[['Incident Code', 'Incident Category']][:10])
        print(df[:10])

    def get_fbi_code_from_crime_code(self, city):
        df = pd.read_csv(os.path.join(cd.data_folder, cd.code_folder, f'{city}_crime_code.csv'))
        df['fbi_code'] = df['offense_code'].astype(str).str.zfill(4).str[:2]
        df.to_csv(os.path.join(cd.data_folder, cd.code_folder, f'{city}_crime_code.csv'))

if __name__ == '__main__':
    cd = CrimeData()
    #cd.save_crime_codes_to_file('boston', code_column='OFFENSE_CODE', description_column='OFFENSE_DESCRIPTION')
    #cd.save_crime_codes_to_file('chicago', code_column='FBI Code', description_column='Primary Type')
    #cd.save_crime_codes_to_file('dc', description_column='OFFENSE')
    cd.save_crime_codes_to_file('la', code_column='Crm Cd Desc', description_column='Crm Cd')
    #cd.save_crime_codes_to_file('boston', description_column='PD_DESC', code_column='OFNS_DESC')
    #cd.save_crime_codes_to_file('seattle', description_column='Description', code_column='Primary Type')
    #cd.save_crime_codes_to_file('sf', description_column='Incident Category', code_column='Incident Code')
    #cd.explore_crime_data('boston')
    #cd.save_crime_codes_to_file('austin', description_column='Highest Offense Description',
    #                            code_column='Highest Offense Code')
    # the following files follow FBI coding system and can be easily merged:
    # chicago, dc, seattle
    # the following cities seem to follow the same coding system but need to be verified:
    # nyc, la
    # sf's coding also looks easy to deal with
    #cd.get_fbi_code_from_crime_code(city='austin')
    #cd.get_fbi_code_from_crime_code(city='boston')



