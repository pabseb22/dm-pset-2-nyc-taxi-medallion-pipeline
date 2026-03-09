if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
import pandas as pd
from sqlalchemy import create_engine, text


def log(msg: str):
    print(f'[BRONZE_ZONES] {msg}')


def get_env(name: str, default=None):
    value = os.getenv(name)
    if value is None or value == '':
        return default
    return value


def get_postgres_engine():
    host = get_env('POSTGRES_HOST', 'postgres')
    port = get_env('POSTGRES_PORT', '5432')
    db = get_env('POSTGRES_DB')
    user = get_env('POSTGRES_USER')
    password = get_env('POSTGRES_PASSWORD')

    if not all([host, port, db, user, password]):
        raise ValueError('Postgres connection variables are incomplete')

    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    return create_engine(conn_str, pool_pre_ping=True, future=True)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


def ensure_bronze_objects(conn):
    conn.execute(text('CREATE SCHEMA IF NOT EXISTS bronze;'))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bronze.ingest_audit (
            year_month VARCHAR(7),
            service_type VARCHAR(10),
            status TEXT,
            row_count BIGINT,
            source_url TEXT,
            error_message TEXT,
            ingest_ts TIMESTAMP
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bronze.taxi_zone_lookup (
            locationid INTEGER,
            borough TEXT,
            zone TEXT,
            service_zone TEXT,
            ingest_ts TIMESTAMP NOT NULL
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bronze.yellow_trips_raw (
            ingest_ts TIMESTAMP NOT NULL,
            source_month VARCHAR(7) NOT NULL,
            service_type VARCHAR(10) NOT NULL
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bronze.green_trips_raw (
            ingest_ts TIMESTAMP NOT NULL,
            source_month VARCHAR(7) NOT NULL,
            service_type VARCHAR(10) NOT NULL
        );
    """))


def ensure_indexes(conn):
    stmts = [
        """
        CREATE INDEX IF NOT EXISTS idx_bronze_yellow_month_service
        ON bronze.yellow_trips_raw (source_month, service_type)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_bronze_green_month_service
        ON bronze.green_trips_raw (source_month, service_type)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_bronze_audit_month_service
        ON bronze.ingest_audit (year_month, service_type)
        """
    ]
    for stmt in stmts:
        try:
            conn.execute(text(stmt))
        except Exception as e:
            log(f'Index warning: {str(e)[:150]}')


@custom
def transform_custom(*args, **kwargs):
    zone_url = get_env('NYC_TLC_ZONE_URL')
    if not zone_url:
        raise ValueError('NYC_TLC_ZONE_URL is missing')

    ingest_ts = pd.Timestamp.utcnow().tz_localize(None)
    log('RUN START')

    zones_df = pd.read_csv(zone_url)
    zones_df = normalize_columns(zones_df)
    zones_df['ingest_ts'] = ingest_ts

    engine = get_postgres_engine()
    with engine.begin() as conn:
        ensure_bronze_objects(conn)

        conn.execute(text('TRUNCATE TABLE bronze.taxi_zone_lookup;'))
        zones_df.to_sql(
            'taxi_zone_lookup',
            conn,
            schema='bronze',
            if_exists='append',
            index=False,
            chunksize=1000,
            method='multi',
        )

        ensure_indexes(conn)

    log(f'ZONES REPLACED rows={len(zones_df):,}')
    log('RUN END')

    return {
        'zones_rows': len(zones_df),
        'status': 'ok',
    }


@test
def test_output(output, *args):
    assert output is not None
    assert output['status'] == 'ok'