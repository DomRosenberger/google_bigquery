# -*- coding: utf-8 -*-
"""Base BigQueryService class wrapping the main BigQuery API functionality.

The API calls are being retried times by using the num_retries kwarg.
"""

import logging

from googleapiclient.errors import HttpError
from google_bigquery.common.utils import MillisecondsSinceEpoch

LOGGER = logging.getLogger(__name__)

DEFAULT_MAX_BQ_ROWS = 100000
DEFAULT_MIN_BQ_ROWS = 10000  # Minimum BigQuery rows to request.


class Error(Exception):
    """Base class for exceptions."""


class BigQueryInterfaceError(Error):
    """Required fields are missing error."""


class _BigQueryBase(object):
    """BigQuery wrapper with basic function for table manipulations."""

    def __init__(self, bq_service, bq_api_retries):
        """Initializes _BigQueryBase object."""
        self.bq_service = bq_service
        self.bq_api_retries = bq_api_retries

    def _execute_api_request(self, bq_request):
        """Executes the API request.

        Args:
            bq_request: BigQuery request object.
        Returns:
            A table resource in a resource body if API call was successful.
            See also: https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        response = bq_request.execute(num_retries=self.bq_api_retries)
        LOGGER.debug('Executed BigQuery request, response: %s', response)
        return response

    def insert_table(self, project_id, dataset_id, table_id,
                     table_schema, expire_in_hours=None):
        """Inserts data to a new BigQuery table.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_id: Str, the BigQuery table ID.
            table_schema: {'fields': [{str: str}]}, the BigQuery table schema. E.g.
                {'fields': [{'type': 'STRING', 'name': 'campaign', 'mode': 'REQUIRED'}]}
            expire_in_hours: Int, the expiry time in hours for the create BigQuery table.
        Returns:
            A table resource in a resource body if API call was successful.
            See also: https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        table_ref = {
            'projectId': project_id,
            'datasetId': dataset_id,
            'tableId': table_id,
        }
        request_body = {
            'tableReference': table_ref,
            'schema': table_schema,
        }
        if expire_in_hours:
            request_body['expirationTime'] = MillisecondsSinceEpoch(expire_in_hours)

        bq_request = self.bq_service.tables().insert(
            projectId=project_id,
            datasetId=dataset_id,
            body=request_body
        )
        return self._execute_api_request(bq_request)

    def has_table(self, project_id, dataset_id, table_id):
        """Whether a table with the table_id exists or not.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_id: Str, the BigQuery table ID.
        Returns:
            A tuple of true and the table resource object if table exists.
            A tuple of false and None if there was another error. See also
            https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        try:
            bq_request = self.bq_service.tables().get(
                projectId=project_id,
                datasetId=dataset_id,
                tableId=table_id,
            )
            response = self._execute_api_request(bq_request)
            return (True, response)
        except HttpError, err:
            if err.resp.status == 404:
                LOGGER.debug('404 not found error, meaning the table does not exist')
                return (False, None)
            else:
                LOGGER.error('Error: %s', err)
            raise err

    def delete_table(self, project_id, dataset_id, table_id, check_if_exists=True):
        """Deletes an existing BigQuery table.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_name: Str, the BigQuery table ID.
            check_if_exists: Boolean, whether to check if table exists before deleting it.
        Returns:
            {}, if successful, it returns an empty response body.
            See also: https://cloud.google.com/bigquery/docs/reference/v2/tables/delete
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        if check_if_exists:
            has_table, unused_response = self.has_table(project_id, dataset_id, table_id)
            if not has_table:
                return True

        bq_request = self.bq_service.tables().delete(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
        )
        return self._execute_api_request(bq_request)

    def get_table(self, project_id, dataset_id, table_id):
        """Gets the table resource.

        This method does not return the data in the table,
        it only returns the table resource, which describes
        the structure of this table

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_name: Str, the BigQuery table ID.
        Returns:
            The table resource object, False if the table does not exist,
            and None if there was another error. See also
            https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        bq_request = self.bq_service.tables().get(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id)
        return self._execute_api_request(bq_request)

    def list_tables(self, project_id, dataset_id):
        """List all table data in a specific dataset.

        See: https://cloud.google.com/bigquery/docs/reference/v2/tables/list

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
        Returns:
            BigQuery body response object.
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        bq_request = self.bq_service.tables().list(
            projectId=project_id,
            datasetId=dataset_id)
        return self._execute_api_request(bq_request)

    def get_table_schema(self, project_id, dataset_id, table_id):
        """Returns Schema from a BigQuery Table.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_name: Str, the BigQuery table ID.
        Returns:
            {'fields': [{str: str}]}, the Schema of the BigQuery table.
        Raises:
            HttpError: If something went wrong with the API call and/or retries failed.
            See also: https://cloud.google.com/bigquery/troubleshooting-errors
        """
        table_ref = {
            'projectId': project_id,
            'datasetId': dataset_id,
            'tableId': table_id,
        }
        bq_request = self.bq_service.tables().get(**table_ref)
        table_info_response = self._execute_api_request(bq_request)
        return table_info_response.get('schema', {})

    def get_table_rows(self, project_id, dataset_id, table_id,
                       max_rows=DEFAULT_MAX_BQ_ROWS):
        """Returns rows from BigQuery table with max_rows restriction.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_name: Str, the BigQuery table ID.
            max_rows: Int, the maximum number of rows to return.
        Returns:
            [[str, str]], a nested list where each list contains all values from each
            row represented as a unicode string.
            Example: [[u'Dave', u'Smith', u'25', u'New York'],
                      [u'Andrew', u'Tayler', u'30', 'Chicago']]
        """
        table_ref = {
            'projectId': project_id,
            'datasetId': dataset_id,
            'tableId': table_id,
        }
        rows = []
        while len(rows) < max_rows:
            bq_request = self.bq_service.tabledata().list(
                maxResults=min(DEFAULT_MIN_BQ_ROWS, max_rows - len(rows)),
                startIndex=len(rows), **table_ref
            )
            data = self._execute_api_request(bq_request)
            max_rows = min(max_rows, int(data['totalRows']))

            more_rows = data.get('rows', [])
            for row in more_rows:
                rows.append([entry.get('v', '') for entry in row.get('f', [])])
            if not more_rows and len(rows) != max_rows:
                raise BigQueryInterfaceError(
                    'Not enough rows returned by server')
        return rows

    def update_table_view_query(self, project_id, dataset_id, view_table_id, query):
        """Updates the view query of a table.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_name: Str, the BigQuery table ID.
            query: Str, the query to update the table with.
        Returns:
            A table resource in a resource body if API call was successful.
            See also: https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        """
        request_body = {
            'view': {
                'query': query,
            }
        }
        LOGGER.debug(
            'Updating table view with: %s for '
            'dataset_id: %s', request_body, dataset_id)

        bq_request = self.bq_service.tables().patch(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=view_table_id,
            body=request_body
        )
        return self._execute_api_request(bq_request)

    def insert_json_rows_to_table(self, project_id, dataset_id, table_id, json_rows,
                                  skip_invalid_rows=True, ignore_unknown_values=True):
        """Inserts a list of json_rows to BigQuery table.

        Args:
            project_id: Str, the BigQuery project ID.
            dataset_id: Str, the BigQuery dataset ID.
            table_name: Str, the BigQuery table ID.
            json_rows: [{'json': {'field_name': 'field_value'}}], list of json rows.
            skip_invalid_rows: Bool, optional flag whether to insert all valid rows of a request,
                even if invalid rows exist. Default: true.
            ignore_unknown_values: Bool, optional flag whether unknown rows should be
                ignored. Default true.
        Returns:
            A table resource in a resource body if API call was successful.
            See also: https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        """
        # Generate a unique job_id so retries
        # don't accidentally duplicate query
        request_body = {
            'kind': 'bigquery#tableDataInsertAllRequest',
            'skipInvalidRows': skip_invalid_rows,
            'ignoreUnknownValues': ignore_unknown_values,
            'rows': json_rows,
        }

        # https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
        bq_request = self.bq_service.tabledata().insertAll(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=request_body
        )
        return self._execute_api_request(bq_request)
