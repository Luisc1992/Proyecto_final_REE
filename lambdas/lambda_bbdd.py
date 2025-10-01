import boto3
import pandas as pd
import pg8000  
import os
from io import StringIO

# Configuración RDS
RDS_HOST = "proyectohabreee.cv4ea0syasip.eu-north-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DBNAME = "postgres"
RDS_USER = "postgres"
RDS_PASSWORD = "proyectohabree123"

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    if not key.endswith('.csv'):
        print(f"Archivo {key} no es CSV, se omite.")
        return

    # Leer CSV desde S3
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    df.rename(columns={'datetime': 'fecha', 
        'value': 'demanda_mw'}, inplace=True)

    print(df.columns.tolist())

    try:
        # Conectar a RDS
        conn = pg8000.connect(
            host=RDS_HOST,
            port=RDS_PORT,
            database=RDS_DBNAME,
            user=RDS_USER,
            password=RDS_PASSWORD
        )
        cursor = conn.cursor()
        print("Conexión a RDS exitosa")
        
        upsert_sql = """
            INSERT INTO demanda_ree (fecha, demanda_mw)
            VALUES (%s, %s)
            ON CONFLICT (fecha) DO UPDATE
            SET demanda_mw = EXCLUDED.demanda_mw;
        """
        for _, row in df.iterrows():
            cursor.execute(upsert_sql, (row['fecha'], row['demanda_mw']))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Datos del archivo {key} insertados correctamente.")
    except Exception as e:
        print(f"Error conectando o insertando en RDS: {str(e)}")
        raise e
