import pandas as pd
import datetime
from covid_lib import bigquery_interface as cbq
from covid_lib import functions as f
import numpy as np

global_filename = 'global_jhu_data.csv'
us_filename = 'us_jhu_data.csv'
global_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/" \
             "csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
us_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/" \
         "csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"


def get_states_data():
    return pd.read_csv('states.csv')


def initial_data_process(url, filename):
    """Takes a URL and filename. Gets the data from that URL and saves it to that filename,
    then does basic processing like renaming columns and pivoting to be an actual time series.
    """
    if f.get_and_save_data(url, filename):
        df = pd.read_csv(filename)
    else:
        raise IOError("Data I/O error")

    # Drop Lat and Long as we don't need this
    # First do it for global data
    if 'Long' in df.columns:
        df = df.drop(['Lat', 'Long'], axis=1)
    # Then for the US data, which has way more to drop
    else:
        df = df.drop(['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Lat', 'Long_', 'Combined_Key'], axis=1)

    # Rename columns to remove / - BQ doesn't like that
    df = df.rename({'Province/State': 'Province_State', 'Country/Region': 'Country_Region'}, axis=1)

    # Data is in a time series where each date is a column - pivot so dates are in rows
    pivoted_df = pd.melt(df, ['Province_State', 'Country_Region'], var_name='Date', value_name='Cases')
    pivoted_df.loc[:, 'Date'] = pivoted_df['Date'].apply(lambda x: datetime.datetime.strptime(x, '%m/%d/%y').date())

    return pivoted_df


def primary_process():
    # Get the global data and drop the US - we'll process that separately
    global_data = initial_data_process(global_url, global_filename)
    global_data = global_data[(global_data['Country_Region'] != 'United States') &
                              (global_data['Country_Region'] != 'US')]

    # Process the US data
    us_data = initial_data_process(us_url, us_filename)

    # Get rid of the county- and city-level data - condense into state by state. Rename "US" to "United States"
    us_data = us_data.groupby(['Country_Region', 'Province_State', 'Date']).sum().reset_index()
    us_data.loc[:, 'Country_Region'] = 'United States'

    # Combine DFs and sort. Obtain daily cases number.
    final_df = pd.concat([global_data, us_data])
    final_df.sort_values(['Country_Region', 'Province_State', 'Date'], inplace=True)
    final_df = final_df.reset_index().drop(['index'], axis=1)

    final_df[['Yesterday', 'Yesterday_country', 'Yesterday_province']] = \
        final_df[['Cases', 'Country_Region', 'Province_State']].shift(1)

    # must iterate through rows - other methods haven't worked well
    # todo change this to df.apply()
    for i, row in final_df.iterrows():
        if row['Yesterday_country'] == row['Country_Region'] and \
                (row['Province_State'] is np.nan or row['Yesterday_province'] == row['Province_State']):
            final_df.loc[i, 'Daily_Cases'] = row['Cases'] - row['Yesterday']
        else:
            final_df.loc[i, 'Daily_Cases'] = 0

    # Correct schema and write to BQ
    final_df = final_df.drop(['Yesterday_country', 'Yesterday_province', 'Yesterday'], axis=1)
    cbq.write_df_to_bq('cases_data', 'Date', ['Country_Region'], final_df)


if __name__ == '__main__':
    primary_process()
