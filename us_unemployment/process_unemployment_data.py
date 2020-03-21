import pandas as pd
import requests
import os
import datetime
from google.cloud import bigquery

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


def download_data():
    r = requests.get(data_url)
    if r.status_code == 200:
        with open(filename, 'w') as f:
            f.writelines(r.text)
    else:
        raise IOError('Error getting data: {}'.format(r.reason))


def load_data():
    # If the data is stale (over 24 hours old) re-download
    if (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(filename))) > \
            datetime.timedelta(hours=24):
        download_data()
    data_df = pd.read_csv(filename, na_values=" ")
    data_df.rename(columns, axis=1, inplace=True)
    for column in ['rptdate', 'week_ending', 'curdate', 'prior3wk', 'priorwk']:
        data_df[column] = pd.to_datetime(data_df[column], format='%m/%d/%Y')
        data_df[column] = data_df[column].dt.date
    return data_df


df = load_data()

# Write to BQ
client = bigquery.Client.from_service_account_json("C:\\tb-covid-19.json")
table_id = 'covid19_data.us_unemployment'
job_config = bigquery.LoadJobConfig(autodetect=True,
                                    time_partitioning=bigquery.table.TimePartitioning(field='week_ending'),
                                    clustering_fields=['st'],
                                    write_disposition='WRITE_TRUNCATE')
load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

print('Load job status: {}, {} rows loaded'.format(load_job.result().state, load_job.result().output_rows))
