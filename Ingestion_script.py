import pandas as pd
import os

#gcloud libraries
from google.cloud import storage
from google.cloud import bigquery



database =  ['Shipments','Trackers','Details','Address']

def upload_to_gcs():

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB



    for tables in database:
        table = pd.read_excel('pangea.xlsx',sheet_name=tables)

        table.to_csv(f"{tables}.csv",index=False)


        client = storage.Client()
        bucket = client.bucket("shipments_records_datalake_project_endless-context-338620")

        blob = bucket.blob(f"database_tables/{tables}.csv")
        blob.upload_from_filename(f"{tables}.csv")

        print(f"{tables}.csv successfully uploaded")

        os.remove(f"{tables}.csv")



def upload_to_big_query():

    

    for tables in database:
        

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = f"endless-context-338620.Shipment_Records.{tables}"

        # TODO(developer): Set uri to the path of the kind export metadata
        uri = (
            f"gs://shipments_records_datalake_project_endless-context-338620/database_tables/{tables}.csv"
        )
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format='CSV',
            autodetect=True)
          

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))


def main():
    upload_to_gcs()
    upload_to_big_query()


if __name__ == '__main__':
    main()
