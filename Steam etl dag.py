# Student ID : 202311127
# Project    : DEA Stage 6 — Final ETL Pipeline
# Dataset    : Steam Games

import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


RAW_PATH     = "/usr/local/airflow/data/steam.csv"
CLEANED_PATH = "/tmp/steam_cleaned.csv"

PG_HOST     = "host.docker.internal"
PG_PORT     = 5435
PG_DB       = "steam_db"
PG_USER     = "postgres"
PG_PASSWORD = "Amman"


default_args = {
    "owner"       : "202311127",
    "retries"     : 1,
    "retry_delay" : timedelta(minutes=5),
    "start_date"  : datetime(2024, 1, 1),
}

def extract():
    df = pd.read_csv(RAW_PATH)
    print(f"[EXTRACT] Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"[EXTRACT] Columns: {df.columns.tolist()}")
    return True


def transform():
    df = pd.read_csv(RAW_PATH)

    # 1. Drop duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["appid"])
    print(f"[TRANSFORM] Duplicates removed: {before - len(df)}")

    # 2. Handle nulls
    df["developer"]  = df["developer"].fillna("Unknown")
    df["publisher"]  = df["publisher"].fillna("Unknown")
    df["categories"] = df["categories"].fillna("")
    df["genres"]     = df["genres"].fillna("")

    # 3. Parse release_date
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["release_year"] = df["release_date"].dt.year.fillna(0).astype(int)

    df["owners_lower"] = (
        df["owners"]
        .str.split("-").str[0]
        .str.replace(",", "")
        .astype(float)
        .astype(int)
    )

    total_ratings = df["positive_ratings"] + df["negative_ratings"]
    df["sentiment_ratio"] = (
        df["positive_ratings"] / total_ratings.replace(0, 1)
    ).round(4)


    import numpy as np
    for col in ["positive_ratings", "negative_ratings", "average_playtime", "owners_lower"]:
        df[f"{col}_log"] = np.log1p(df[col])


    df = df[df["price"] >= 0]


    df["is_free"] = (df["price"] == 0).astype(int)

    df.to_csv(CLEANED_PATH, index=False)
    print(f"[TRANSFORM] Saved {len(df)} cleaned rows to {CLEANED_PATH}")


def load():
    import psycopg2
    from sqlalchemy import create_engine

    df = pd.read_csv(CLEANED_PATH)

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    df.to_sql("steam_games", engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        count = conn.execute(__import__("sqlalchemy").text("SELECT COUNT(*) FROM steam_games")).scalar()

    print(f"[LOAD] Loaded {count} rows into steam_games table in PostgreSQL")

with DAG(
    dag_id="steam_etl_dag",
    default_args=default_args,
    description="Steam Games ETL Pipeline — Extract, Transform, Load",
    schedule="0 2 * * *",
    catchup=False,
    tags=["dea", "steam", "etl"],
) as dag:

    t1_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t2_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t3_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t1_extract >> t2_transform >> t3_load