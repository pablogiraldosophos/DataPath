from google.cloud import bigquery

# Inicializar el cliente de BigQuery
client = bigquery.Client()

# Llamar al procedimiento almacenado
query = "CALL `terpel-pruebas-369613.DataPath_QTY.sp_orquestador`();"
config = bigquery.QueryJobConfig()


query_job = client.query(query, job_config=config)
results = query_job.result()