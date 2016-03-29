"""Tests for bigquery_base.py.

I tried initially to use the HttpMock from apiclient.http to mock the BigQuery
service but it always threw the following error.
KeyError: "rootUrl" in discovery.py line 313.

Running the example from following site seems to work just fine though.
https://developers.google.com/api-client-library/python/guide/mocks#example
"""

import json
import mox
from google_bigquery.bigquery_base import _BigQueryBase, DEFAULT_MIN_BQ_ROWS, \
    DEFAULT_MAX_BQ_ROWS
from google_bigquery.bigquery_service import DEFAULT_NUM_API_RETRIES

FAKE_PROJECT_ID = 'bigquery_project_id'
FAKE_DATASET_ID = 'bigquery_dataset_id'
FAKE_TABLE_ID = 'bigquery_table_id'

FAKE_TABLE_SCHEMA = {
    'fields': [
        {'type': 'STRING', 'name': 'firstname', 'mode': 'REQUIRED'},
        {'type': 'STRING', 'name': 'surname', 'mode': 'REQUIRED'},
        {'type': 'INTEGER', 'name': 'age', 'mode': 'NULLABLE'},
    ]
}

TABLE_INSERT_SUCCESS_FILE = 'google_bigquery/tests/data/insert_success.json'
TABLE_DELETE_SUCCESS_FILE = 'google_bigquery/tests/data/delete_success.json'
TABLE_GET_SUCCESS_FILE = 'google_bigquery/tests/data/delete_success.json'
TABLE_DATA_LIST_SUCCESS_FILE = 'google_bigquery/tests/data/table_data_list_success.json'


class BigQueryBaseTest(mox.MoxTestBase):
    """Testing _BigQueryBase object."""

    def setUp(self):
        super(BigQueryBaseTest, self).setUp()
        self.project_id = FAKE_PROJECT_ID
        self.dataset_id = FAKE_DATASET_ID
        self.table_id = FAKE_TABLE_ID
        self.table_ref = {
            'projectId': self.project_id,
            'datasetId': self.dataset_id,
            'tableId': self.table_id,
        }
        self.request_body = {
            'tableReference': self.table_ref,
            'schema': FAKE_TABLE_SCHEMA,
        }

    def test_insert_table_success(self):
        """Tests a successful table insert."""
        bq_service_mock = self._mock_table_insert()
        self.mox.ReplayAll()

        bq_service_obj = _BigQueryBase(bq_service_mock, DEFAULT_NUM_API_RETRIES)

        response = bq_service_obj.insert_table(
            self.project_id,
            self.dataset_id,
            self.table_id,
            FAKE_TABLE_SCHEMA
        )
        self.assertEquals(u'TABLE', response.get('type'))
        self.assertEquals(u'bigquery#table', response.get('kind'))

    def test_delete_table_success(self):
        """Test a successful table deletion without check for existing table."""
        bq_service_mock = self._mock_table_delete()
        self.mox.ReplayAll()

        bq_service_obj = _BigQueryBase(bq_service_mock, DEFAULT_NUM_API_RETRIES)

        response = bq_service_obj.delete_table(
            self.project_id,
            self.dataset_id,
            self.table_id,
            check_if_exists=False,
        )
        self.assertEquals({}, response)

    def test_has_table_success(self):
        """Test function for existence of table."""
        bq_service_mock = self._mock_get_table()
        self.mox.ReplayAll()

        bq_service_obj = _BigQueryBase(bq_service_mock, DEFAULT_NUM_API_RETRIES)

        response = bq_service_obj.has_table(
            self.project_id,
            self.dataset_id,
            self.table_id,
        )
        self.assertTrue(response)

    def test_get_table_rows(self):
        """Test function to get table rows."""
        expected_response = [
            [u'Dave', u'Smith', u'25', u'New York'],
            [u'Andrew', u'Tayler', u'30', u'Chicago'],
            [u'Tim', u'Woods', u'35', u'Orlando']
        ]

        bq_service_mock = self._mock_table_data_list()
        # bq_service_mock = self._mock_get_table()
        self.mox.ReplayAll()

        bq_service_obj = _BigQueryBase(bq_service_mock, DEFAULT_NUM_API_RETRIES)

        response = bq_service_obj.get_table_rows(
            self.project_id,
            self.dataset_id,
            self.table_id,
        )
        self.assertEquals(expected_response, response)

    def _mock_execute(self, json_file):
        json_response = None
        with open(json_file, 'r') as api_resp:
            json_response = json.loads(api_resp.read())
        # Mock execute function call.
        bq_execute_mock = self.mox.CreateMockAnything()
        bq_execute_mock.execute(num_retries=DEFAULT_NUM_API_RETRIES).AndReturn(json_response)
        return bq_execute_mock

    def _mock_table_insert(self):
        bq_service_mock = self.mox.CreateMockAnything()

        bq_table_service_mock = self.mox.CreateMockAnything()
        bq_execute_mock = self._mock_execute(TABLE_INSERT_SUCCESS_FILE)
        # Mock insert function call.
        bq_table_service_mock.insert(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            body=self.request_body
        ).AndReturn(bq_execute_mock)

        bq_service_mock.tables().AndReturn(bq_table_service_mock)
        return bq_service_mock

    def _mock_table_delete(self):
        bq_service_mock = self.mox.CreateMockAnything()

        bq_table_service_mock = self.mox.CreateMockAnything()
        bq_execute_mock = self._mock_execute(TABLE_DELETE_SUCCESS_FILE)
        # Mock delete function call.
        bq_table_service_mock.delete(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            tableId=self.table_id,
        ).AndReturn(bq_execute_mock)

        bq_service_mock.tables().AndReturn(bq_table_service_mock)
        return bq_service_mock

    def _mock_get_table(self):
        bq_service_mock = self.mox.CreateMockAnything()

        bq_table_service_mock = self.mox.CreateMockAnything()
        bq_execute_mock = self._mock_execute(TABLE_GET_SUCCESS_FILE)
        # Mock get function call.
        bq_table_service_mock.get(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            tableId=self.table_id,
        ).AndReturn(bq_execute_mock)

        bq_service_mock.tables().AndReturn(bq_table_service_mock)
        return bq_service_mock

    def _mock_table_data_list(self):
        bq_service_mock = self.mox.CreateMockAnything()

        bq_table_data_service_mock_1 = self.mox.CreateMockAnything()
        bq_execute_mock = self._mock_execute(TABLE_DATA_LIST_SUCCESS_FILE)
        # Mock list function call.
        bq_table_data_service_mock_1.list(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            tableId=self.table_id,
            maxResults=min(DEFAULT_MIN_BQ_ROWS, DEFAULT_MAX_BQ_ROWS - 0),
            startIndex=0
        ).AndReturn(bq_execute_mock)

        bq_service_mock.tabledata().AndReturn(bq_table_data_service_mock_1)
        return bq_service_mock
