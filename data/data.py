import requests
import pandas as pd
import datetime
from google.cloud import bigquery

filename = 'data.csv'
url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"


def get_states_data():
    return pd.read_csv('states.csv')


def get_and_save_data():
    r = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(r.content)
    return True


# The data for the states is a weird amalgam of county data and state data that isn't consistent over time
# Extract the county data and put it into the state, since that data is available starting 3/8
def process_states(df: pd.DataFrame):
    states_aka = {x[1].strip(): x[0] for x in get_states_data().values}

    def translate_states(value):
        if ',' in value:
            state = value.split(',')[-1].strip()
            return states_aka[state]
        else:
            return value

    # Get all US values
    states_df = df[df['Country_Region'] == 'US']

    # Convert the Province_State column to just be state, removing county/city info
    states_df.loc[:, 'Province_State'] = states_df['Province_State'].apply(lambda x: translate_states(x))

    # Sum by state and date
    states_df = states_df.groupby(['Province_State', 'Date']).agg({'Cases': 'sum'}).reset_index()

    # Re-add the Country_Region value
    states_df.loc[:, 'Country_Region'] = 'US'

    # Remove all US values from the original DF
    df = df[df['Country_Region'] != 'US']

    # Concatenate new US values with old non-US values
    return pd.concat([df, states_df])


if __name__ == '__main__':
    if get_and_save_data():
        data_df = pd.read_csv(filename)
    else:
        raise IOError("Data I/O error")

    # Drop Lat and Long as we don't need this
    data_df = data_df.drop(['Lat', 'Long'], axis=1)
    
    # Rename columns to remove / - BQ doesn't like that
    data_df = data_df.rename({'Province/State': 'Province_State', 'Country/Region': 'Country_Region'}, axis=1)

    # Data is in a time series where each date is a column - pivot so dates are in rows
    pivoted_df = pd.melt(data_df, ['Province_State', 'Country_Region'], var_name='Date', value_name='Cases')
    pivoted_df.loc[:, 'Date'] = pivoted_df['Date'].apply(lambda x: datetime.datetime.strptime(x, '%m/%d/%y').date())

    # Fix US data
    final_df = process_states(pivoted_df)

    # Write to BQ
    client = bigquery.Client()
    table_id = 'covid19.cases_data'
    job_config = bigquery.LoadJobConfig(autodetect=True,
                                        time_partitioning=bigquery.table.TimePartitioning(field='Date'),
                                        clustering_fields=['Country_Region'],
                                        write_disposition='WRITE_TRUNCATE')
    load_job = client.load_table_from_dataframe(final_df, table_id, job_config=job_config)

    print('Load job status: {}, {} rows loaded'.format(load_job.result().state, load_job.result().output_rows))
