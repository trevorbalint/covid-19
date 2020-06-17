import pandas as pd
import os
import datetime
from covid_lib import bigquery_interface as cbq
from covid_lib import functions as f

data_url = 'https://oui.doleta.gov/unemploy/csv/ar539.csv'
filename = 'doleta_data.csv'

# columns data from https://oui.doleta.gov/dmstree/handbooks/402/402_4/4024c6/4024c6.pdf#ETA539-ar539
columns = {'c1': 'week_number', 'c2': 'week_ending',
           'c3': 'ic', 'c4': 'fic',
           'c5': 'xic', 'c6': 'wsic',
           'c7': 'wseic', 'c8': 'cw',
           'c9': 'fcw', 'c10': 'xcw',
           'c11': 'wscw', 'c12': 'wsecw',
           'c13': 'ebt', 'c14': 'ebui',
           'c15': 'abt', 'c16': 'abui',
           'c17': 'at', 'c18': 'ce',
           'c19': 'r', 'c20': 'ar',
           'c21': 'p', 'c22': 'status',
           'c23': 'status_change_date'}


def load_data():
    # If the data is stale (over 24 hours old) re-download
    if (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(filename))) > \
            datetime.timedelta(hours=24):
        f.get_and_save_data(data_url, filename)
    data_df = pd.read_csv(filename, na_values=" ")
    data_df.rename(columns, axis=1, inplace=True)
    for column in ['rptdate', 'week_ending', 'curdate', 'priorwk']:
        data_df[column] = pd.to_datetime(data_df[column], format='%m/%d/%Y')
        data_df[column] = data_df[column].dt.date
    return data_df


def primary_process():
    df = load_data()

    # Write to BQ
    cbq.write_df_to_bq('us_unemployment', 'week_ending', ['st'], df)


if __name__ == '__main__':
    primary_process()
