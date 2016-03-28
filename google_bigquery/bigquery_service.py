"""BigQuery service module."""

import logging

from google_bigquery.bigquery_base import _BigQueryBase

LOGGER = logging.getLogger(__name__)

DEFAULT_NUM_API_RETRIES = 3
DEFAULT_QUERY_TIMEOUT_MS = 30 * 1000  # 30 seconds.
DEFAULT_CHUNK_SIZE = 1000


class BigQueryService(_BigQueryBase):
    """BigQuery API service extending basic functions."""

    def __init__(self, service, bq_api_retries=DEFAULT_NUM_API_RETRIES):
        super(BigQueryService, self).__init__(service, bq_api_retries)

    def get_all_table_ids(self, project_id, dataset_id):
        """Returns a set of all tables ID names.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
        Returns:
            Set(str), a set of all table ID names (tableId).
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        response = self.list_tables(project_id, dataset_id)

        get_list_request = {
            'projectId': project_id,
            'datasetId': dataset_id,
        }

        table_names = set()
        if response:
            while True:
                tables = response.get('tables', [])
                for table in tables:
                    table_ref = table.get('tableReference')
                    if table_ref:
                        table_id = table_ref.get('tableId')
                        table_names.add(table_id)

                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

                get_list_request['pageToken'] = page_token
                bq_request = self.bq_service.tables().list(**get_list_request)
                response = self._execute_api_request(bq_request)

        logging.debug('table_names: %s', table_names)
        return table_names

    def get_query_results_as_string(
        self, project_id, query, timeout=DEFAULT_QUERY_TIMEOUT_MS,
            chunk_size=DEFAULT_CHUNK_SIZE):
        """Returns query result as comma separated string.

        Args:
            project_id: Str, the BigQuery project ID.
            query: Str, the BigQuery query.
            timeout: Int, the query timeout in milliseconds.
            chunk_size: Int, the chunk size for retrieving query result chunks.
        Returns:
            Str, the query result as a comma separating string for each table row.
            Example: 'Dave,Smith,25,New York\nAndrew,Tayler,30,Chicago'.
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        query_request = {
            'query': query,
            'timeoutMs': timeout,
            'maxResults': chunk_size,
            'alt': 'csv',
        }
        LOGGER.debug('Running query: %s with body: %s', query, query_request)

        bq_request = self.bq_service.jobs().query(
            projectId=project_id,
            body=query_request)
        response = self._execute_api_request(bq_request)
        job_ref = response['jobReference']

        get_results_request = {
            'projectId': project_id,
            'jobId': job_ref['jobId'],
            'timeoutMs': timeout,
            'maxResults': chunk_size,
            'alt': 'csv',
        }
        content = ''
        while True:
            page_token = response.get('pageToken', None)
            query_complete = response['jobComplete']
            if query_complete:
                content += self.get_query_content_as_string(response)
                if page_token is None:
                    logging.info('No new page_token, break loop!')
                    break

            # Set the page token so that we know where to start reading from.
            get_results_request['pageToken'] = page_token
            logging.info('Running get_results_request: %s', get_results_request)
            bq_request = self.bq_service.jobs().getQueryResults(**get_results_request)
            response = self._execute_api_request(bq_request)
        return content

    def get_query_content_as_string(self, response_body):
        """Returns BigQuery result object as comma separated string.

        Args:
            response_body: {response_body}, the BigQuery response body for a query.
                See: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query.
        Returns:
            Str, the query result as a comma separating string for each table row.
            Example: 'Dave,Smith,25,New York\nAndrew,Tayler,30,Chicago'.
        """
        fields = response_body['schema']['fields']
        rows = response_body['rows']
        content = ''

        for row in rows:
            one_row = []
            for i in xrange(0, len(fields)):
                cell = row['f'][i]
                # field = fields[i]
                val = cell['v'] or '-1'  # Store -1 if None
                one_row.append(val)
            if one_row:
                content += ','.join(one_row) + '\n'

        return content
