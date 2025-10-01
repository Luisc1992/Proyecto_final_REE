from fastapi import FastAPI
from pydantic import BaseModel
import google.generativeai as genai
import psycopg2


genai.configure(api_key="AIzaSyCwPnbsn8xlIfkbakm3hHN3I3rgTXx0TFU")  

app = FastAPI()

class Question(BaseModel):
    question: str


DB_CONFIG = {
    "host": "proyectohabreee.cv4ea0syasip.eu-north-1.rds.amazonaws.com",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "proyectohabree123"
}

def clean_sql(sql_text: str) -> str:
    """
    Limpia el SQL generado por el LLM:
    - Elimina ```sql o ``` del principio/final
    - Quita espacios extra
    """
    sql_text = sql_text.strip()
    if sql_text.startswith("```sql"):
        sql_text = sql_text[len("```sql"):].strip()
    if sql_text.startswith("```"):
        sql_text = sql_text[3:].strip()
    if sql_text.endswith("```"):
        sql_text = sql_text[:-3].strip()
    return sql_text

@app.post("/ask")
async def ask_endpoint(payload: Question):
    user_question = payload.question

    prompt = f"""
    Eres un generador de SQL.
    Base de datos: usa la estructura existente de mi base de datos PostgreSQL.
    Base de Datos de nombre postgres
    -Tabla:
    demanda_ree(fecha,demanda_mw,id)


  "question": "string",
  "fecha_inicio": "YYYY-MM-DD o null",
  "fecha_fin": "YYYY-MM-DD o null",
  -El valor de fecha es un timestamp con formato YYYY-MM-DD HH:MM:SS.Ten en cuenta solo el YYYY-MM-DD para la consulta

    Pregunta del usuario:
    {user_question}

    Devuelve únicamente la consulta SQL, sin explicaciones.
    """

    try:
        
        model = genai.GenerativeModel("gemini-2.5-flash") 
        response = model.generate_content(prompt)
        sql_generated = clean_sql(response.text)

       
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql_generated)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

       
        results = [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        return {"error": str(e), "sql_generated": sql_generated}

    return {
        "question": user_question,
        "sql_generated": sql_generated,
        "results": results
    }


