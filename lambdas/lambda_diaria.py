import requests
import csv
import datetime
import boto3
from datetime import datetime, timedelta, timezone

s3 = boto3.client("s3")
BUCKET_NAME = "proyectohabree"

def lambda_handler(event, context):

  
    now = datetime.now(timezone.utc)
    ayer = (now - timedelta(days=1)).date()

    start_date = f"{ayer}T00:00Z" 
    end_date = f"{ayer}T23:59Z"

    prefix = f"demanda_diaria/{ayer}/"

    
    url = "https://apidatos.ree.es/es/datos/demanda/evolucion"
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "time_trunc": "day",
        "geo_limit": "peninsular"
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Crear CSV 
    values = data.get("included", [])[0].get("attributes", {}).get("values", [])
    if not values:
        print("No hay datos para la península en este día")
        return {"statusCode": 200, "body": "Sin datos"}
    
    filtered = [
        row for row in values
        if datetime.fromisoformat(row["datetime"].replace("Z", "+00:00")).date() == ayer
    ]

    if not filtered:
        print("No hay datos para la península en este día")
        return {"statusCode": 200, "body": "Sin datos"}

    ruta_local = f"/tmp/demanda_peninsula_{ayer}.csv"
    with open(ruta_local, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["datetime", "value", "percentage"])
        writer.writeheader()
        writer.writerows(filtered)

    ruta_s3 = prefix + "demanda_peninsula.csv"
    s3.upload_file(ruta_local, BUCKET_NAME, ruta_s3)
    
    print(f"Subido: s3://{BUCKET_NAME}/{ruta_s3}")

    return {
        "statusCode": 200,
        "body": f"Archivo diario de la península guardado en s3://{BUCKET_NAME}/{ruta_s3}"
    }
