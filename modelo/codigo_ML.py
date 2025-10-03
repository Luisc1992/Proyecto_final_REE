import os
import pandas as pd
import numpy as np
import joblib
from datetime import timedelta
from sqlalchemy import create_engine
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Configuración BBDD

DB_HOST = os.getenv("DB_HOST", "proyectohabreee.cv4ea0syasip.eu-north-1.rds.amazonaws.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "proyectohabree123")
TABLE_NAME = "demanda_ree"

# Rutas
MODEL_PATH = "../modelo/lgb_demand_model.joblib"
FEATURES_PATH = "../modelo/features_cols.txt"

# Funciones


def load_data_from_rds(limit_rows=None):
    """Conecta a la BBDD y devuelve un DataFrame con columnas: id, fecha, demanda_mw"""
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str, connect_args={"connect_timeout": 10})
    query = f"SELECT id, fecha, demanda_mw FROM {TABLE_NAME} ORDER BY fecha ASC"
    if limit_rows:
        query += f" LIMIT {limit_rows}"
    df = pd.read_sql(query, engine)
    return df

def preprocess_and_features(df):
    """Convierte fecha a datetime, crea features temporales y lags/rolling"""
    df = df.copy()
    df['fecha'] = pd.to_datetime(df['fecha'])
    df = df.sort_values('fecha').drop_duplicates(subset='fecha').reset_index(drop=True)
    
    df['dayofweek'] = df['fecha'].dt.dayofweek
    df['month'] = df['fecha'].dt.month
    df['is_weekend'] = df['dayofweek'].isin([5,6]).astype(int)
    
    # Lags
    for l in [1, 7, 14, 30]:
        df[f'lag_{l}'] = df['demanda_mw'].shift(l)
    
    # Rolling
    for w in [7, 14, 30]:
        df[f'roll_mean_{w}'] = df['demanda_mw'].shift(1).rolling(window=w, min_periods=1).mean()
        df[f'roll_std_{w}'] = df['demanda_mw'].shift(1).rolling(window=w, min_periods=1).std().fillna(0)
    
    df = df.dropna().reset_index(drop=True)
    return df

def predict_future(model, last_df, features, horizon=7):
    """
    last_df: DataFrame con los últimos datos
    horizon: número de días a predecir
    """
    last_df = last_df.copy().sort_values('fecha').reset_index(drop=True)
    preds = []
    df_work = last_df.copy()
    
    for step in range(horizon):
        next_time = df_work['fecha'].iloc[-1] + pd.Timedelta(days=1)
        row = {"fecha": next_time}
        row['dayofweek'] = next_time.dayofweek
        row['month'] = next_time.month
        row['is_weekend'] = int(next_time.dayofweek in [5,6])
        
        # Lags
        for l in [1,7,14,30]:
            row[f'lag_{l}'] = df_work['demanda_mw'].iloc[-l] if l <= len(df_work) else np.nan
        
        # Rolling
        for w in [7,14,30]:
            vals = df_work['demanda_mw'].shift(1).iloc[-w:] if len(df_work) >= 1 else []
            if len(vals) > 0:
                row[f'roll_mean_{w}'] = float(vals.mean())
                row[f'roll_std_{w}'] = float(vals.std()) if len(vals) > 1 else 0.0
            else:
                row[f'roll_mean_{w}'] = np.nan
                row[f'roll_std_{w}'] = np.nan
        
        row_df = pd.DataFrame([row])
        for c in features:
            if c not in row_df.columns:
                row_df[c] = np.nan
        row_df = row_df[features].fillna(df_work[features].mean())
        pred = model.predict(row_df)[0]
        
        new_row = {"fecha": next_time, "demanda_mw": pred}
        preds.append(new_row)
        df_work = pd.concat([df_work, pd.DataFrame([new_row])], ignore_index=True)
    
    return pd.DataFrame(preds)
