#!/usr/bin/env python3
from google.cloud import bigquery

client = bigquery.Client()

QUERY = (
    'SELECT V2Tone, DocumentIdentifier '
    'FROM `gdelt-bq.gdeltv2.gkg` '
    'WHERE V2Themes LIKE "%ECON_INFLATION%" '
    'LIMIT 10')

query_job = client.query(QUERY)
rows = query_job.result()

for row in rows:
    print(row.V2Tone, row.DocumentIdentifier)