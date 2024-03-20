import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# update the variables below
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/my-creds.json'
project_id = 'woven-edge-412500'
bucket_name = 'woven-edge-412500-de-retail-sales-bucket'
object_key = 'retail_data.parquet'
table_name = 'retail_data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data, *args, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        filesystem=gcs
    )