import os
import pandas as pd
from tqdm import tqdm

nyc_crime_file = '../../data/crime_data/NY_Complaint_Data_from_2006_to_2019.csv'
df = pd.read_csv(nyc_crime_file)
print('read csv to file')
df = df['CMPLNT_FR_DT']
flag = []
date_list = df.to_list()
for date in tqdm(date_list, total=len(date_list)):
    try:
        year = pd.to_datetime(date).year
        flag.append(year >= 2014)
    except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
        flag.append(False)

df = pd.read_csv(nyc_crime_file)
print('read csv to file')
df = df[flag]
new_filename = '../../data/crime_data/NY_Complaint_Data_from_2014_to_2019.csv'
df.to_csv(new_filename, index=False)