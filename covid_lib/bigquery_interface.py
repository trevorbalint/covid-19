from google.cloud import bigquery
import pandas as pd


def write_df_to_bq(table_name: str, partitioning_field: str, clustering_fields: [str], df: pd.DataFrame):
    # Write to BQ
    client = bigquery.Client.from_service_account_json("C:\\tb-covid-19.json")
    table_id = 'covid19_data.{}'.format(table_name)
    job_config = bigquery.LoadJobConfig(autodetect=True,
                                        time_partitioning=bigquery.table.TimePartitioning(field=partitioning_field),
                                        clustering_fields=clustering_fields,
                                        write_disposition='WRITE_TRUNCATE')
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    print('Load job status: {}, {} rows loaded'.format(load_job.result().state, load_job.result().output_rows))
