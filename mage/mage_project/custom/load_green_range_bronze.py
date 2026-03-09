if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import csv
import gc
import os
import tempfile
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text


SERVICE_TYPE = 'green'

TRIP_METADATA_COLUMNS = {
    'ingest_ts': 'TIMESTAMP NOT NULL',
    'source_month': 'VARCHAR(7) NOT NULL',
    'service_type': 'VARCHAR(10) NOT NULL',
}


def log(msg: str):
    print(f'[BRONZE_GREEN] {msg}')


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


def month_range(start_month: str, end_month: str):
    start = pd.Period(start_month, freq='M')
    end = pd.Period(end_month, freq='M')
    if start > end:
        raise ValueError('INGEST_START_MONTH cannot be greater than INGEST_END_MONTH')
    return [m.strftime('%Y-%m') for m in pd.period_range(start=start, end=end, freq='M')]


def build_trip_url(base_url: str, service_type: str, year_month: str) -> str:
    return f'{base_url}/{service_type}_tripdata_{year_month}.parquet'


def download_to_temp_file(url: str, suffix: str = '.parquet') -> str:
    response = requests.get(url, stream=True, timeout=180)
    if response.status_code == 404:
        raise FileNotFoundError(f'File not found: {url}')
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                tmp.write(chunk)
        return tmp.name


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


def optimize_trip_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    nullable_int_candidates = [
        'vendorid', 'ratecodeid', 'pulocationid', 'dolocationid',
        'payment_type', 'passenger_count', 'trip_type',
    ]
    for col in nullable_int_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')

    float_candidates = [
        'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
        'tolls_amount', 'improvement_surcharge', 'total_amount',
        'congestion_surcharge', 'airport_fee',
    ]
    for col in float_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')

    for col in ['lpep_pickup_datetime', 'lpep_dropoff_datetime']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'store_and_fwd_flag' in df.columns:
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('string')

    return df


def df_mem_mb(df: pd.DataFrame) -> float:
    return round(df.memory_usage(deep=True).sum() / 1024**2, 2)


def map_dtype_to_postgres(series: pd.Series) -> str:
    dtype = str(series.dtype).lower()
    if 'datetime' in dtype:
        return 'TIMESTAMP'
    if dtype in ('int64', 'int32', 'int16', 'int8', 'uint64', 'uint32', 'uint16', 'uint8'):
        return 'BIGINT'
    if dtype in ('float64', 'float32'):
        return 'DOUBLE PRECISION'
    if dtype == 'bool':
        return 'BOOLEAN'
    return 'TEXT'


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def get_existing_columns(conn, schema: str, table: str):
    result = conn.execute(
        text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
        """),
        {'schema': schema, 'table': table}
    )
    return [row[0] for row in result.fetchall()]


def create_table_if_not_exists_for_df(conn, schema: str, table: str, df: pd.DataFrame):
    column_defs = []
    for col in df.columns:
        col_type = TRIP_METADATA_COLUMNS[col] if col in TRIP_METADATA_COLUMNS else map_dtype_to_postgres(df[col])
        column_defs.append(f'{quote_ident(col)} {col_type}')

    ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table} ({", ".join(column_defs)});'
    conn.execute(text(ddl))


def add_missing_columns(conn, schema: str, table: str, df: pd.DataFrame):
    existing_cols = set(get_existing_columns(conn, schema, table))
    missing_cols = [c for c in df.columns if c not in existing_cols]

    for col in missing_cols:
        col_type = TRIP_METADATA_COLUMNS[col] if col in TRIP_METADATA_COLUMNS else map_dtype_to_postgres(df[col])
        conn.execute(text(f'ALTER TABLE {schema}.{table} ADD COLUMN {quote_ident(col)} {col_type};'))


def align_df_to_table(conn, schema: str, table: str, df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    table_cols = get_existing_columns(conn, schema, table)
    for col in table_cols:
        if col not in df.columns:
            df[col] = None
    return df[table_cols]


def copy_dataframe_to_postgres(conn, df: pd.DataFrame, schema: str, table: str):
    raw_conn = conn.connection
    cursor = raw_conn.cursor()
    buffer = StringIO()

    copy_df = df.copy()
    for col in copy_df.columns:
        if pd.api.types.is_datetime64_any_dtype(copy_df[col]):
            copy_df[col] = copy_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            copy_df[col] = copy_df[col].astype(object)

    copy_df = copy_df.where(pd.notnull(copy_df), None)
    copy_df.to_csv(buffer, index=False, header=False, sep=',', na_rep='', quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    columns_sql = ', '.join([quote_ident(c) for c in copy_df.columns])
    sql = f"""
        COPY {schema}.{table} ({columns_sql})
        FROM STDIN WITH (
            FORMAT CSV,
            DELIMITER ',',
            NULL '',
            QUOTE '"',
            ESCAPE '"'
        )
    """
    cursor.copy_expert(sql, buffer)
    cursor.close()


def upsert_audit_row(conn, row: dict):
    conn.execute(
        text("""
            DELETE FROM bronze.ingest_audit
            WHERE year_month = :year_month
              AND service_type = :service_type
        """),
        {'year_month': row['year_month'], 'service_type': row['service_type']}
    )

    pd.DataFrame([row]).to_sql(
        'ingest_audit',
        conn,
        schema='bronze',
        if_exists='append',
        index=False,
        chunksize=1000,
        method='multi',
    )


def process_month(conn, base_url: str, year_month: str, ingest_ts: pd.Timestamp):
    url = build_trip_url(base_url, SERVICE_TYPE, year_month)
    table_name = 'green_trips_raw'
    temp_path = None

    log(f'START month={year_month}')

    try:
        temp_path = download_to_temp_file(url)
        temp_size_mb = round(Path(temp_path).stat().st_size / 1024**2, 2)
        log(f'DOWNLOADED month={year_month} temp_mb={temp_size_mb}')

        df = pd.read_parquet(temp_path)
        log(f'PARQUET READ month={year_month} rows={len(df):,}')

        df = optimize_trip_dtypes(df)
        df['ingest_ts'] = ingest_ts
        df['source_month'] = year_month
        df['service_type'] = SERVICE_TYPE

        row_count = int(len(df))
        mem_mb = df_mem_mb(df)
        log(f'TYPED month={year_month} rows={row_count:,} mem_mb={mem_mb}')

        create_table_if_not_exists_for_df(conn, 'bronze', table_name, df)
        add_missing_columns(conn, 'bronze', table_name, df)

        conn.execute(
            text("""
                DELETE FROM bronze.green_trips_raw
                WHERE source_month = :source_month
                  AND service_type = :service_type
            """),
            {'source_month': year_month, 'service_type': SERVICE_TYPE}
        )

        aligned_df = align_df_to_table(conn, 'bronze', table_name, df)
        copy_dataframe_to_postgres(conn, aligned_df, 'bronze', table_name)

        audit_row = {
            'year_month': year_month,
            'service_type': SERVICE_TYPE,
            'status': 'loaded',
            'row_count': row_count,
            'source_url': url,
            'error_message': None,
            'ingest_ts': ingest_ts,
        }
        upsert_audit_row(conn, audit_row)

        log(f'LOADED month={year_month} inserted={row_count:,}')

        del aligned_df
        del df
        gc.collect()

        return {'status': 'loaded', 'row_count': row_count}

    except FileNotFoundError as e:
        upsert_audit_row(conn, {
            'year_month': year_month,
            'service_type': SERVICE_TYPE,
            'status': 'missing',
            'row_count': 0,
            'source_url': url,
            'error_message': str(e),
            'ingest_ts': ingest_ts,
        })
        log(f'MISSING month={year_month}')
        return {'status': 'missing', 'row_count': 0}

    except Exception as e:
        upsert_audit_row(conn, {
            'year_month': year_month,
            'service_type': SERVICE_TYPE,
            'status': 'failed',
            'row_count': 0,
            'source_url': url,
            'error_message': str(e)[:1000],
            'ingest_ts': ingest_ts,
        })
        log(f'FAILED month={year_month} error={str(e)[:180]}')
        return {'status': 'failed', 'row_count': 0}

    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
        gc.collect()


@custom
def transform_custom(*args, **kwargs):
    base_url = get_env('NYC_TLC_BASE_URL')
    start_month = get_env('INGEST_START_MONTH')
    end_month = get_env('INGEST_END_MONTH')

    if not base_url:
        raise ValueError('NYC_TLC_BASE_URL is missing')
    if not start_month or not end_month:
        raise ValueError('INGEST_START_MONTH / INGEST_END_MONTH are missing')

    months = month_range(start_month, end_month)
    ingest_ts = pd.Timestamp.utcnow().tz_localize(None)

    log(f'RUN START range={start_month}->{end_month} months={len(months)}')

    results = []
    engine = get_postgres_engine()

    with engine.begin() as conn:
        for ym in months:
            result = process_month(conn, base_url, ym, ingest_ts)
            results.append((ym, result))

            log(
                f'MONTH SUMMARY month={ym} '
                f'status={result["status"]} rows_loaded={result["row_count"]:,}'
            )

    loaded_files = sum(1 for _, r in results if r['status'] == 'loaded')
    missing_files = sum(1 for _, r in results if r['status'] == 'missing')
    failed_files = sum(1 for _, r in results if r['status'] == 'failed')
    total_rows_loaded = sum(r['row_count'] for _, r in results if r['status'] == 'loaded')

    log(
        f'RUN END loaded={loaded_files} missing={missing_files} '
        f'failed={failed_files} rows_loaded={total_rows_loaded:,}'
    )

    return {
        'service_type': SERVICE_TYPE,
        'months_processed': len(months),
        'loaded_files': loaded_files,
        'missing_files': missing_files,
        'failed_files': failed_files,
        'total_rows_loaded': total_rows_loaded,
    }


@test
def test_output(output, *args):
    assert output is not None
    assert output['service_type'] == 'green'