from google.cloud import bigquery
import pandas as pd


# Simply returns a bigquery client - helper function to remove the need to import bigquery everywhere, and keep code DRY
def get_bq_client():
    return bigquery.Client.from_service_account_json("C:\\tb-covid-19.json")


# Writes a dataframe to the covid19_data dataset in bigquery
# desired table name is a string
# partitioning field must be a date or timestamp column or this function will fail
# if partitioning_field is None, you cannot have clustering_fields
# truncate decides whether to append to existing table or overwrite existing table
def write_df_to_bq(table_name: str, partitioning_field: str,
                   clustering_fields: [str], df: pd.DataFrame,
                   truncate: bool = True):
    # Get the client
    client = get_bq_client()
    table_id = 'covid19_data.{}'.format(table_name)

    # Translate boolean to bigquery-speak
    if truncate:
        write_disposition = 'WRITE_TRUNCATE'
    else:
        write_disposition = 'WRITE_APPEND'

    # Create the job config - different if there is are partitioning/clustering fields
    if partitioning_field:
        job_config = bigquery.LoadJobConfig(autodetect=True,
                                            time_partitioning=bigquery.table.TimePartitioning(field=partitioning_field),
                                            clustering_fields=clustering_fields,
                                            write_disposition=write_disposition)
    else:
        job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition=write_disposition)

    # Run the job and output status
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    print('Load job status: {}, {} rows loaded'.format(load_job.result().state, load_job.result().output_rows))


# Given a table name as string, check if that table exists in the covid19_data dataset in bigquery or not
def check_table_existence(table_name: str):
    client = get_bq_client()
    existing_tables = [x.table_id for x in client.list_tables('covid19_data')]
    return table_name in existing_tables
