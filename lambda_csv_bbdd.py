import boto3
import pandas as pd
import psycopg2  
import os
from io import StringIO

# Configuración RDS
RDS_HOST = "proyectohabreedatabase.cv4ea0syasip.eu-north-1.rds.amazonaws.com"
RDS_PORT = 5432  
RDS_DBNAME = "proyectohabreedatabase"
RDS_USER = "bbddree"
RDS_PASSWORD = "tigresrectos"

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Obtener información del archivo subido
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Solo procesar archivos CSV
    if not key.endswith('.csv'):
        print(f"Archivo {key} no es CSV, se omite.")
        return

    # Leer CSV desde S3
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    # Conectar a RDS
    try:
        conn = psycopg2.connect(
            host=RDS_HOST,
            database=RDS_DBNAME,
            user=RDS_USER,
            password=RDS_PASSWORD,
            port=RDS_PORT
        )
        cursor = conn.cursor()
        print(f"Conexión a RDS exitosa")

        # Insertar datos (ejemplo genérico)
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO tabla_destino (col1, col2, col3) VALUES (%s, %s, %s)",
                (row['col1'], row['col2'], row['col3'])
            )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Datos del archivo {key} insertados correctamente.")
    except Exception as e:
        print(f"Error conectando o insertando en RDS: {str(e)}")
        raise e
