import functions_framework
import pandas as pd
from google.cloud import bigquery
import mysql.connector

@functions_framework.http
def DataPath(request):

    list_tablas = ["customers", "employees", "offices", "orders", "payments", "productlines", "products" ]

    for i in list_tablas:
        print (i)

        # Configuración de MySQL
        mysql_host = '34.125.182.227'
        mysql_port = "3306" 
        mysql_database = 'classicmodels'
        mysql_user = 'alumnos'
        mysql_password = '123456789ABC'

        # Configuración de BigQuery
        bigquery_project_id = 'terpel-pruebas-369613'
        bigquery_dataset_id = 'DataPath_RAW'
        bigquery_table_id = str("my_sql_"+i+"_raw") #listar las tablas que se van a consultar en la db Mysql

        # Conexión a MySQL
        mysql_connection = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            database=mysql_database,
            user=mysql_user,
            password=mysql_password
        )

        # Consulta de las tablas en MySQL
        mysql_query = str ("SELECT * FROM " + i)
        mysql_cursor = mysql_connection.cursor()
        mysql_cursor.execute(mysql_query)
        mysql_result = mysql_cursor.fetchall()

        # Creación de un DataFrame de pandas con los datos de MySQL
        df = pd.DataFrame(mysql_result, columns=mysql_cursor.column_names)

        # Autenticación de Google Cloud y conexión a BigQuery
        bigquery_client = bigquery.Client(project=bigquery_project_id)

        # Envío de datos a BigQuery
        table_ref = bigquery_client.dataset(bigquery_dataset_id).table(bigquery_table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

    return ("Datos enviados correctamente a BigQuery.")

