# if 'data_loader' not in globals():
#     from mage_ai.data_preparation.decorators import data_loader
# if 'test' not in globals():
#     from mage_ai.data_preparation.decorators import test

# import os
# from io import BytesIO
# import pandas as pd
# import requests


# SERVICE_TYPES = ['yellow', 'green']


# def month_range(start_month: str, end_month: str):
#     start = pd.Period(start_month, freq='M')
#     end = pd.Period(end_month, freq='M')

#     if start > end:
#         raise ValueError('INGEST_START_MONTH cannot be greater than INGEST_END_MONTH')

#     return [m.strftime('%Y-%m') for m in pd.period_range(start=start, end=end, freq='M')]


# def build_trip_url(base_url: str, service_type: str, year_month: str) -> str:
#     return f'{base_url}/{service_type}_tripdata_{year_month}.parquet'


# def download_parquet(url: str) -> pd.DataFrame:
#     response = requests.get(url, timeout=180)
#     if response.status_code == 404:
#         raise FileNotFoundError(f'File not found: {url}')
#     response.raise_for_status()
#     return pd.read_parquet(BytesIO(response.content))


# def optimize_trip_dtypes(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Reduce memoria sin romper compatibilidad entre años/servicios.
#     """
#     df = df.copy()
#     df.columns = [c.lower() for c in df.columns]

#     # Integers nullable comunes
#     nullable_int_candidates = [
#         'vendorid', 'ratecodeid', 'pulocationid', 'dolocationid',
#         'payment_type', 'passenger_count'
#     ]
#     for col in nullable_int_candidates:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')

#     # Floats comunes
#     float_candidates = [
#         'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
#         'tolls_amount', 'improvement_surcharge', 'total_amount',
#         'congestion_surcharge', 'airport_fee'
#     ]
#     for col in float_candidates:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')

#     # Strings pequeñas/repetidas
#     for col in ['store_and_fwd_flag']:
#         if col in df.columns:
#             df[col] = df[col].astype('string')

#     return df


# @data_loader
# def load_data(*args, **kwargs):
#     base_url = os.getenv('NYC_TLC_BASE_URL')
#     zone_url = os.getenv('NYC_TLC_ZONE_URL')
#     start_month = os.getenv('INGEST_START_MONTH')
#     end_month = os.getenv('INGEST_END_MONTH')

#     if not base_url:
#         raise ValueError('NYC_TLC_BASE_URL is missing')
#     if not zone_url:
#         raise ValueError('NYC_TLC_ZONE_URL is missing')
#     if not start_month or not end_month:
#         raise ValueError('INGEST_START_MONTH / INGEST_END_MONTH are missing')

#     ingest_ts = pd.Timestamp.utcnow().tz_localize(None)

#     trip_batches = []
#     audit_rows = []

#     months = month_range(start_month, end_month)
#     print(f'Loading range: {start_month} -> {end_month} ({len(months)} months)')
#     print(f'Services: {SERVICE_TYPES}')

#     for ym in months:
#         for service_type in SERVICE_TYPES:
#             url = build_trip_url(base_url, service_type, ym)
#             print(f'[START] {service_type} {ym} -> {url}')

#             try:
#                 df = download_parquet(url)
#                 df = optimize_trip_dtypes(df)

#                 df['ingest_ts'] = ingest_ts
#                 df['source_month'] = ym
#                 df['service_type'] = service_type

#                 row_count = int(len(df))
#                 mem_mb = round(df.memory_usage(deep=True).sum() / 1024**2, 2)

#                 trip_batches.append({
#                     'service_type': service_type,
#                     'source_month': ym,
#                     'row_count': row_count,
#                     'columns': list(df.columns),
#                     'data': df.to_dict(orient='records'),
#                 })

#                 audit_rows.append({
#                     'year_month': ym,
#                     'service_type': service_type,
#                     'status': 'loaded',
#                     'row_count': row_count,
#                     'source_url': url,
#                     'error_message': None,
#                     'ingest_ts': ingest_ts,
#                 })

#                 print(f'[OK] {service_type} {ym}: rows={row_count:,} mem_mb={mem_mb}')

#             except FileNotFoundError as e:
#                 audit_rows.append({
#                     'year_month': ym,
#                     'service_type': service_type,
#                     'status': 'missing',
#                     'row_count': 0,
#                     'source_url': url,
#                     'error_message': str(e),
#                     'ingest_ts': ingest_ts,
#                 })
#                 print(f'[MISSING] {service_type} {ym}: {e}')

#             except Exception as e:
#                 audit_rows.append({
#                     'year_month': ym,
#                     'service_type': service_type,
#                     'status': 'failed',
#                     'row_count': 0,
#                     'source_url': url,
#                     'error_message': str(e)[:1000],
#                     'ingest_ts': ingest_ts,
#                 })
#                 print(f'[FAILED] {service_type} {ym}: {str(e)[:300]}')

#     zones_df = pd.read_csv(zone_url)
#     zones_df.columns = [c.lower() for c in zones_df.columns]
#     zones_df['ingest_ts'] = ingest_ts

#     audit_df = pd.DataFrame(audit_rows)
#     audit_df.columns = [c.lower() for c in audit_df.columns]

#     if not trip_batches:
#         raise ValueError('No trip files were loaded for any service/month')

#     loaded_batches = sum(1 for r in audit_rows if r['status'] == 'loaded')
#     loaded_rows = sum(r['row_count'] for r in audit_rows if r['status'] == 'loaded')

#     print(f'trip_batches loaded: {loaded_batches}')
#     print(f'total rows loaded: {loaded_rows:,}')
#     print(f'zones rows: {len(zones_df):,}')
#     print(f'audit rows: {len(audit_df):,}')

#     return {
#         'trip_batches': trip_batches,
#         'zones_df': zones_df.to_dict(orient='records'),
#         'audit_df': audit_df.to_dict(orient='records'),
#     }


# @test
# def test_output(output, *args) -> None:
#     assert output is not None, 'The output is undefined'
#     assert 'trip_batches' in output, 'Missing trip_batches'
#     assert 'zones_df' in output, 'Missing zones_df'
#     assert 'audit_df' in output, 'Missing audit_df'
#     assert len(output['audit_df']) > 0, 'audit_df is empty'