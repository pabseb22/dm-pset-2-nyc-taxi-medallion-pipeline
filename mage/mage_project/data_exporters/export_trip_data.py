# if 'data_exporter' not in globals():
#     from mage_ai.data_preparation.decorators import data_exporter

# import os
# import pandas as pd
# from sqlalchemy import create_engine, text


# TRIP_METADATA_COLUMNS = {
#     'ingest_ts': 'TIMESTAMP NOT NULL',
#     'source_month': 'VARCHAR(7) NOT NULL',
#     'service_type': 'VARCHAR(10) NOT NULL',
# }


# def get_secret_or_env(name: str, default=None):
#     value = os.getenv(name)
#     if value is None or value == '':
#         return default
#     return value


# def get_postgres_engine():
#     host = get_secret_or_env('POSTGRES_HOST', 'postgres')
#     port = get_secret_or_env('POSTGRES_PORT', '5432')
#     db = get_secret_or_env('POSTGRES_DB')
#     user = get_secret_or_env('POSTGRES_USER')
#     password = get_secret_or_env('POSTGRES_PASSWORD')

#     if not all([host, port, db, user, password]):
#         raise ValueError('Postgres connection variables are incomplete')

#     conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
#     return create_engine(
#         conn_str,
#         pool_pre_ping=True,
#         pool_size=5,
#         max_overflow=10
#     )


# def quote_ident(name: str) -> str:
#     return '"' + str(name).replace('"', '""') + '"'


# def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
#     df = df.copy()
#     df.columns = [c.lower() for c in df.columns]
#     return df


# def map_dtype_to_postgres(series: pd.Series) -> str:
#     dtype = str(series.dtype).lower()

#     if 'datetime' in dtype:
#         return 'TIMESTAMP'
#     if dtype in ('int64', 'int32', 'int16', 'int8', 'uint64', 'uint32', 'uint16', 'uint8'):
#         return 'BIGINT'
#     if dtype in ('float64', 'float32'):
#         return 'DOUBLE PRECISION'
#     if dtype == 'bool':
#         return 'BOOLEAN'
#     return 'TEXT'


# def create_table_if_not_exists_for_df(conn, schema: str, table: str, df: pd.DataFrame):
#     column_defs = []
#     for col in df.columns:
#         if col in TRIP_METADATA_COLUMNS:
#             col_type = TRIP_METADATA_COLUMNS[col]
#         else:
#             col_type = map_dtype_to_postgres(df[col])
#         column_defs.append(f'{quote_ident(col)} {col_type}')

#     ddl = f"""
#         CREATE TABLE IF NOT EXISTS {schema}.{table} (
#             {", ".join(column_defs)}
#         );
#     """
#     conn.execute(text(ddl))


# def get_existing_columns(conn, schema: str, table: str):
#     result = conn.execute(
#         text("""
#             SELECT column_name
#             FROM information_schema.columns
#             WHERE table_schema = :schema
#               AND table_name = :table
#             ORDER BY ordinal_position
#         """),
#         {'schema': schema, 'table': table}
#     )
#     return [row[0] for row in result.fetchall()]


# def add_missing_columns(conn, schema: str, table: str, df: pd.DataFrame):
#     existing_cols = set(get_existing_columns(conn, schema, table))
#     missing_cols = [c for c in df.columns if c not in existing_cols]

#     for col in missing_cols:
#         if col in TRIP_METADATA_COLUMNS:
#             col_type = TRIP_METADATA_COLUMNS[col]
#         else:
#             col_type = map_dtype_to_postgres(df[col])

#         ddl = f'ALTER TABLE {schema}.{table} ADD COLUMN {quote_ident(col)} {col_type};'
#         conn.execute(text(ddl))


# def align_df_to_table(conn, schema: str, table: str, df: pd.DataFrame) -> pd.DataFrame:
#     df = normalize_columns(df)
#     table_cols = get_existing_columns(conn, schema, table)

#     for col in table_cols:
#         if col not in df.columns:
#             df[col] = None

#     return df[table_cols]


# def create_bronze_support_objects(conn):
#     conn.execute(text('CREATE SCHEMA IF NOT EXISTS bronze;'))

#     conn.execute(text("""
#         CREATE TABLE IF NOT EXISTS bronze.ingest_audit (
#             year_month VARCHAR(7),
#             service_type VARCHAR(10),
#             status TEXT,
#             row_count BIGINT,
#             source_url TEXT,
#             error_message TEXT,
#             ingest_ts TIMESTAMP
#         );
#     """))

#     conn.execute(text("""
#         CREATE TABLE IF NOT EXISTS bronze.taxi_zone_lookup (
#             locationid INTEGER,
#             borough TEXT,
#             zone TEXT,
#             service_zone TEXT,
#             ingest_ts TIMESTAMP NOT NULL
#         );
#     """))


# def create_bronze_indexes(conn):
#     conn.execute(text("""
#         CREATE INDEX IF NOT EXISTS idx_yellow_trips_raw_month_service
#         ON bronze.yellow_trips_raw (source_month, service_type);
#     """))
#     conn.execute(text("""
#         CREATE INDEX IF NOT EXISTS idx_green_trips_raw_month_service
#         ON bronze.green_trips_raw (source_month, service_type);
#     """))
#     conn.execute(text("""
#         CREATE INDEX IF NOT EXISTS idx_ingest_audit_month_service
#         ON bronze.ingest_audit (year_month, service_type);
#     """))


# def replace_zone_lookup(conn, zones_df: pd.DataFrame):
#     conn.execute(text('TRUNCATE TABLE bronze.taxi_zone_lookup;'))

#     zones_df.to_sql(
#         'taxi_zone_lookup',
#         conn,
#         schema='bronze',
#         if_exists='append',
#         index=False,
#         chunksize=1000,
#         method='multi',
#     )


# def upsert_audit(conn, audit_df: pd.DataFrame):
#     if audit_df.empty:
#         return

#     month_service_rows = (
#         audit_df[['year_month', 'service_type']]
#         .drop_duplicates()
#         .to_dict(orient='records')
#     )

#     for row in month_service_rows:
#         conn.execute(
#             text("""
#                 DELETE FROM bronze.ingest_audit
#                 WHERE year_month = :year_month
#                   AND service_type = :service_type
#             """),
#             {
#                 'year_month': row['year_month'],
#                 'service_type': row['service_type'],
#             }
#         )

#     audit_df.to_sql(
#         'ingest_audit',
#         conn,
#         schema='bronze',
#         if_exists='append',
#         index=False,
#         chunksize=1000,
#         method='multi',
#     )


# def delete_target_month(conn, table_name: str, source_month: str, service_type: str):
#     conn.execute(
#         text(f"""
#             DELETE FROM bronze.{table_name}
#             WHERE source_month = :source_month
#               AND service_type = :service_type
#         """),
#         {
#             'source_month': source_month,
#             'service_type': service_type,
#         }
#     )


# def load_trip_batch(conn, batch: dict):
#     service_type = batch['service_type']
#     source_month = batch['source_month']
#     table_name = 'yellow_trips_raw' if service_type == 'yellow' else 'green_trips_raw'

#     df = pd.DataFrame(batch['data'])
#     if df.empty:
#         return 0

#     df = normalize_columns(df)

#     create_table_if_not_exists_for_df(conn, 'bronze', table_name, df)
#     add_missing_columns(conn, 'bronze', table_name, df)
#     delete_target_month(conn, table_name, source_month, service_type)

#     aligned_df = align_df_to_table(conn, 'bronze', table_name, df)

#     aligned_df.to_sql(
#         table_name,
#         conn,
#         schema='bronze',
#         if_exists='append',
#         index=False,
#         chunksize=50000,
#         method='multi',
#     )

#     print(f'[INSERTED] {table_name} {source_month}: {len(aligned_df):,} rows')
#     return len(aligned_df)


# @data_exporter
# def export_data(data, *args, **kwargs):
#     trip_batches = data.get('trip_batches', [])
#     zones_df = pd.DataFrame(data.get('zones_df', []))
#     audit_df = pd.DataFrame(data.get('audit_df', []))

#     if not trip_batches:
#         raise ValueError('No trip_batches found in loader output')

#     zones_df = normalize_columns(zones_df)
#     audit_df = normalize_columns(audit_df)

#     engine = get_postgres_engine()

#     yellow_inserted = 0
#     green_inserted = 0

#     with engine.begin() as conn:
#         create_bronze_support_objects(conn)

#         for batch in trip_batches:
#             inserted = load_trip_batch(conn, batch)
#             if batch['service_type'] == 'yellow':
#                 yellow_inserted += inserted
#             else:
#                 green_inserted += inserted

#         if not zones_df.empty:
#             replace_zone_lookup(conn, zones_df)

#         if not audit_df.empty:
#             upsert_audit(conn, audit_df)

#         # crear índices al final, una vez que ya existen las tablas
#         try:
#             create_bronze_indexes(conn)
#         except Exception as e:
#             print(f'Index creation warning: {e}')

#     return {
#         'yellow_inserted': yellow_inserted,
#         'green_inserted': green_inserted,
#         'zones_inserted': len(zones_df),
#         'audit_rows': len(audit_df),
#         'batches_processed': len(trip_batches),
#     }