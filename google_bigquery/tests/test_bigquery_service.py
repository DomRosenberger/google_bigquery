"""Tests for bigquery_service.py."""

import json
import mox
from google_bigquery.bigquery_service import BigQueryService
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

TABLE_LIST_SUCCESS_FILE = 'google_bigquery/tests/data/list_table_success.json'


class BigQueryServiceTest(mox.MoxTestBase):
    """Testing BigQueryService object."""

    def setUp(self):
        super(BigQueryServiceTest, self).setUp()
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

    def test_get_all_table_ids_success(self):
        """Test listing of table IDs."""
        expected_response = set([u'table_id20151106', u'table_id20151107', u'table_id20151105'])

        bq_service_mock = self._mock_list_table()
        self.mox.ReplayAll()

        bq_service_obj = BigQueryService(bq_service_mock)

        response = bq_service_obj.get_all_table_ids(
            self.project_id,
            self.dataset_id,
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

    def _mock_list_table(self):
        bq_service_mock = self.mox.CreateMockAnything()

        bq_table_service_mock = self.mox.CreateMockAnything()
        bq_execute_mock = self._mock_execute(TABLE_LIST_SUCCESS_FILE)
        # Mock get function call.
        bq_table_service_mock.list(
            projectId=self.project_id,
            datasetId=self.dataset_id,
        ).AndReturn(bq_execute_mock)

        bq_service_mock.tables().AndReturn(bq_table_service_mock)
        return bq_service_mock
