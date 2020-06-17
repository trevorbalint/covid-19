from covid_lib import functions as f
from covid_lib import bigquery_interface as cbq
import pandas as pd

data_url = "https://raw.githubusercontent.com/BuzzFeedNews/nics-firearm-background-checks" \
           "/master/data/nics-firearm-background-checks.csv"
filename = "nics_data.csv"


def data_process():
    if f.get_and_save_data(data_url, filename):
        df = pd.read_csv(filename)
    else:
        raise IOError("Data I/O error")

    df['yearmonth'] = df['month']
    df['year'] = df['yearmonth'].apply(lambda x: int(x.split('-')[0]))
    df['month'] = df['yearmonth'].apply(lambda x: int(x.split('-')[1]))

    cbq.write_df_to_bq('firearms_background_checks', '', [], df)


if __name__ == "__main__":
    data_process()
