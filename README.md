google-bigquery
===============

Simple Wrapper for Google's Bigquery Python API.

# Installation

`pip install google-bigquery`

# Basic Usage in App Engine.

```python

import logging
import httplib2

from apiclient.discovery import build
from google.appengine.api import memcache
from google_bigquery import BigQueryService
from oauth2client.contrib.appengine import AppAssertionCredentials


def ConnectGoogleService(scope, service_name, version):
    """Oauth2 Authentication."""
    logging.info('Authenticate %s', scope)
    credentials = AppAssertionCredentials(scope=scope)
    http = credentials.authorize(httplib2.Http(memcache))
    logging.info('%s successfully authenticated.', scope)
    service = build(service_name, version, http=http)
    return service

# BigQuery ID's as listed in BigQuery interfacce.
project_id = 'project_id'
dataset_id = 'dataset_id'
table_id = 'table_id'

# Oauth2 authentication.
bq_service = ConnectGoogleService(
    'https://www.googleapis.com/auth/bigquery', 'bigquery', 'v2')
bigquery_service_obj = BigQueryService(bq_service)

# Retrieves rows from table ID.
rows = bigquery_service_obj.get_table_rows(project_id, dataset_id, table_id)
logging.info('Rows: %s', rows)
```
