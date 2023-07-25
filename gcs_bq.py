import functions_framework
from google.cloud import bigquery

@functions_framework.http
def DataPath_gcs(request):

    client = bigquery.Client();

    tabla_bq = "bucket_orderdetails_raw";
    table_id = "terpel-pruebas-369613.DataPath_RAW.bucket_orderdetails_raw";

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("orderNumber", "INTEGER"),
            bigquery.SchemaField("productCode", "STRING"),
            bigquery.SchemaField("quantityOrdered", "INTEGER"),
            bigquery.SchemaField("priceEach", "NUMERIC"),
            bigquery.SchemaField("orderLineNumber", "INTEGER")
            ],
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
    )
    job_config.field_delimiter = ";"
    uri = "gs://datapath_input/orderdetails.csv"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    return ("Datos enviados correctamente a BigQuery.")