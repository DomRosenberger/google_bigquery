"""Tests for utils.py."""

import datetime
import mox

from google_bigquery.common import utils


class UtilsTest(mox.MoxTestBase):
    """Testing utils functions."""

    def testMillisecondsSinceEpoch(self):
        hours = 10
        expected_resonse = (31 * 24 + hours) * 3600 * 1000  # milliseconds of 31 days + 10 hours.
        datetime1 = datetime.datetime.strptime('2016-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
        datetime2 = datetime.datetime.strptime('2015-12-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
        timedelta1 = datetime.timedelta(hours=hours)

        # Stub out the datatime.datetime class.
        self.mox.StubOutWithMock(datetime, 'datetime')
        # Stub out the datatime.timedelta class.
        self.mox.StubOutWithMock(datetime, 'timedelta')

        datetime.datetime.now().AndReturn(datetime1)
        datetime.timedelta(hours=hours).AndReturn(timedelta1)
        datetime.datetime.utcfromtimestamp(0).AndReturn(datetime2)

        self.mox.ReplayAll()
        actual_response = utils.MillisecondsSinceEpoch(hours)
        self.assertEqual(expected_resonse, actual_response)
