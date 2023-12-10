# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import unittest

from gcp.bigquery import BigqueryInsertStream
from unittest import mock

ROW1 = dict(row=1)
ROW2 = dict(row=2)
ROW3 = dict(row=3)


class BigqueryInsertStreamTestCase(unittest.TestCase):
    def setUp(self):
        self.client = mock.MagicMock()
        self.table = 'fake.table'

    @mock.patch('gcp.bigquery.BigqueryInsertStream.flush')
    def test_flush_called_at_end_of_context(self, flush):
        flush.return_value = []
        with BigqueryInsertStream(self.client, self.table) as insert_stream:
            insert_stream.insert([ROW1, ROW2])
        flush.assert_called_once()

    @mock.patch('gcp.bigquery.Bigquery.insert_rows')
    def test_raise_on_errors_from_flush(self, bigquery_insert_rows):
        errors = ['fake error 1', 'fake error 2']
        bigquery_insert_rows.return_value = errors
        try:
            with BigqueryInsertStream(self.client, self.table) as insert_stream:
                insert_stream.insert([ROW1, ROW2])
            self.fail('Expected a BigQueryInsertStream.Error for the errors from flush()')
        except BigqueryInsertStream.Errors as e:
            self.assertIs(errors, e.errors)
        bigquery_insert_rows.assert_called_once()

    @mock.patch('gcp.bigquery.Bigquery.insert_rows')
    def test_test_rows_inserted_at_end_of_context(self, bigquery_insert_rows):
        bigquery_insert_rows.return_value = []
        with BigqueryInsertStream(self.client, self.table) as insert_stream:
            insert_stream.insert([ROW1, ROW2])
            insert_stream.insert(ROW3)
        bigquery_insert_rows.assert_called_once_with(self.client, self.table, [ROW1, ROW2, ROW3])

    @mock.patch('gcp.bigquery.Bigquery.insert_rows')
    def test_test_batch_flushes_when_batch_exceeds_and_at_end_of_context(self, bigquery_insert_rows):
        bigquery_insert_rows.return_value = []
        with BigqueryInsertStream(self.client, self.table, batch_size=2) as insert_stream:
            # Insert 1st row, should not be flushed.
            insert_stream.insert(ROW1)
            bigquery_insert_rows.assert_not_called()
            self.assertEqual(0, insert_stream.total)
            self.assertEqual([ROW1], insert_stream.batch)

            # Insert 2nd row, which hits the batch size.
            insert_stream.insert(ROW2)
            bigquery_insert_rows.called_once_with(self.client, self.table, [ROW1, ROW2])
            self.assertEqual(2, insert_stream.total)
            self.assertEqual([], insert_stream.batch)

            # Insert a 3rd row, which should not be flushed, since the batch was cleared.
            bigquery_insert_rows.reset_mock()
            insert_stream.insert(ROW3)
            bigquery_insert_rows.assert_not_called()

        # The context should flush at the exit.
        bigquery_insert_rows.assert_called_once_with(self.client, self.table, [ROW3])


if __name__ == '__main__':
    unittest.main()
