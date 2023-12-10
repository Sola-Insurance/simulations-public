# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import logging
import unittest

from gcp.bigquery import Bigquery, GCS_FILE_URL_PREFIX, IMPORT_FILE_DEFAULT_FORMAT, INSERT_ROW_RETRY_MAX_RETRIES
from google.cloud.bigquery import SourceFormat
from google.cloud.bigquery.job import WriteDisposition
from unittest.mock import MagicMock, call, patch


class BigqueryTestCase(unittest.TestCase):
    def setUp(self):
        # Disable all logging during these tests, since some of them emit warnings.
        # TODO: I don't love this. Is there a better way to do this?
        self.current_log_level = logging.root.level
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        # Restore the previous logging state.
        logging.root.level = self.current_log_level

    @patch('gcp.bigquery.bigquery.Client')
    def test_client_init_default_parameters(self, gcp_bigquery_client_init):
        """Enforces assumptions of what is passed to the internal client init, if no params are provided."""
        client = Bigquery.client()
        self.assertIs(gcp_bigquery_client_init.return_value, client)
        gcp_bigquery_client_init.assert_called_once_with(project=None, credentials=None)

    @patch('gcp.bigquery.bigquery.Client')
    def test_client_init_with_parameters(self, gcp_bigquery_client_init):
        """Enforces provided parameters are passed down to the internal client init."""
        credentials = 'FAKE CREDENTIALS'
        project_id = 'FAKE PROJECT'
        client = Bigquery.client(project_id=project_id, credentials=credentials)
        self.assertIs(gcp_bigquery_client_init.return_value, client)
        gcp_bigquery_client_init.assert_called_once_with(project=project_id, credentials=credentials)

    @patch('gcp.bigquery.bigquery.TableReference.from_string')
    def test_table_reference_default_parameters(self, gcp_bigquery_tableref_from_string):
        """Enforces what is passed to the internal TableReference.from_string api if no params are passed."""
        table_id = 'FAKE.TABLE'
        table_ref = Bigquery.table_reference(table_id)
        self.assertIs(gcp_bigquery_tableref_from_string.return_value, table_ref)
        gcp_bigquery_tableref_from_string.assert_called_once_with(table_id, default_project=None)

    @patch('gcp.bigquery.bigquery.TableReference.from_string')
    def test_table_reference_with_parameters(self, gcp_bigquery_tableref_from_string):
        """Enforces provided parameters are passed down to the internal TableReference.from_string api."""
        table_id = 'FAKE.TABLE'
        project_id = 'FAKE PROJECT'
        table_ref = Bigquery.table_reference(table_id, project_id=project_id)
        self.assertIs(gcp_bigquery_tableref_from_string.return_value, table_ref)
        gcp_bigquery_tableref_from_string.assert_called_once_with(table_id, default_project=project_id)

    @patch('gcp.bigquery.bigquery.LoadJobConfig')
    def test_import_file_from_cloud_storage__default_parameters(self, load_job_config_init):
        file_path = GCS_FILE_URL_PREFIX + 'fake/file'
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        table = MagicMock()
        load_job = MagicMock()
        job_config = MagicMock()
        load_job_config_init.return_value = job_config
        client.get_table.return_value = table
        client.load_table_from_uri.return_value = load_job

        Bigquery.import_file(client, table_id, file_path)
        client.load_table_from_uri.assert_called_once_with(file_path, table, job_config=job_config)
        load_job_config_init.assert_called_once_with(
            schema=None, source_format=IMPORT_FILE_DEFAULT_FORMAT)
        load_job.result.assert_called_once()
        client.get_table.assert_called_once_with(table_id)

    @patch('gcp.bigquery.bigquery.LoadJobConfig')
    def test_import_file_from_cloud_storage__with_parameters(self, load_job_config_init):
        schema = 'fake schema'
        source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        skip_leading_rows = 1
        file_path = GCS_FILE_URL_PREFIX + 'fake/file'
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        load_job = MagicMock()
        job_config = MagicMock()
        load_job_config_init.return_value = job_config
        client.load_table_from_uri.return_value = load_job

        Bigquery.import_file(client, table_id, file_path,
                             source_format=source_format, schema=schema, skip_leading_rows=skip_leading_rows)
        client.load_table_from_uri.assert_called_once_with(file_path, table_id, job_config=job_config)
        load_job_config_init.assert_called_once_with(schema=schema, source_format=source_format)
        self.assertEqual(skip_leading_rows, job_config.skip_leading_rows)
        load_job.result.assert_called_once()

    @patch('gcp.bigquery.bigquery.LoadJobConfig')
    def test_import_file_from_cloud_storage__with_write_disposition(self, load_job_config_init):
        write_disposition = WriteDisposition.WRITE_TRUNCATE
        file_path = GCS_FILE_URL_PREFIX + 'fake/file'
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        table = MagicMock()
        load_job = MagicMock()
        job_config = MagicMock()
        load_job_config_init.return_value = job_config
        client.load_table_from_uri.return_value = load_job
        client.get_table.return_value = table

        Bigquery.import_file(client, table_id, file_path,
                             write_disposition=write_disposition,
                             ignore_unknown_values=True)
        client.load_table_from_uri.assert_called_once_with(file_path, table, job_config=job_config)
        load_job_config_init.assert_called_once_with(
            schema=None, source_format=IMPORT_FILE_DEFAULT_FORMAT)
        self.assertEqual(write_disposition, job_config.write_disposition)
        self.assertTrue(job_config.ignore_unkown_values)
        load_job.result.assert_called_once()
        client.get_table.assert_called_once_with(table_id)

    @patch('gcp.bigquery.open')
    @patch('gcp.bigquery.bigquery.LoadJobConfig')
    def test_import_file_from_local__default_parameters(self, load_job_config_init, file_open_ctxt):
        file_path = '/path/to/fake/file'
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        table = MagicMock()
        load_job = MagicMock()
        job_config = MagicMock()
        file_stream = MagicMock()
        load_job_config_init.return_value = job_config
        client.load_table_from_file.return_value = load_job
        client.get_table.return_value = table
        file_open_ctxt.return_value.__enter__.return_value = file_stream  # contextmanagers are annoying to mock.

        Bigquery.import_file(client, table_id, file_path)
        client.load_table_from_file.assert_called_once_with(file_stream, table, job_config=job_config)
        load_job_config_init.assert_called_once_with(
            schema=None, source_format=IMPORT_FILE_DEFAULT_FORMAT)
        load_job.result.assert_called_once()
        file_open_ctxt.assert_called_once_with(file_path, 'rb')
        client.get_table.assert_called_once_with(table_id)

    @patch('gcp.bigquery.open')
    @patch('gcp.bigquery.bigquery.LoadJobConfig')
    def test_import_file_from_local__with_parameters(self, load_job_config_init, file_open_ctxt):
        schema = 'fake schema'
        source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        skip_leading_rows = 1
        file_path = '/path/to/fake/file'
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        load_job = MagicMock()
        job_config = MagicMock()
        file_stream = MagicMock()
        load_job_config_init.return_value = job_config
        client.load_table_from_file.return_value = load_job
        file_open_ctxt.return_value.__enter__.return_value = file_stream  # contextmanagers are annoying to mock.

        Bigquery.import_file(client, table_id, file_path,
                             source_format=source_format, schema=schema, skip_leading_rows=skip_leading_rows)
        client.load_table_from_file.assert_called_once_with(file_stream, table_id, job_config=job_config)
        load_job_config_init.assert_called_once_with(schema=schema, source_format=source_format)
        self.assertEqual(skip_leading_rows, job_config.skip_leading_rows)
        load_job.result.assert_called_once()
        file_open_ctxt.assert_called_once_with(file_path, 'rb')

    def test_insert_rows__success(self):
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        table = MagicMock()
        rows = [dict(a=1), dict(b=2), dict(c=3)]
        client.get_table.return_value = table
        client.insert_rows.return_value = []

        self.assertEqual([], Bigquery.insert_rows(client, table_id, rows))
        client.insert_rows.assert_called_once_with(table, rows)

    def test_insert_rows__with_kwargs(self):
        table_id = 'FAKE.TABLE'
        extra_arg = 'fake-arg'
        client = MagicMock()
        table = MagicMock()
        rows = [dict(a=1), dict(b=2), dict(c=3)]
        client.get_table.return_value = table
        client.insert_rows.return_value = []

        self.assertEqual([], Bigquery.insert_rows(client, table_id, rows, extra_arg=extra_arg))
        client.insert_rows.assert_called_once_with(table, rows, extra_arg=extra_arg)

    def test_insert_rows__retry_on_error_ends_with_success(self):
        """Simulate two transient errors, ending with 3rd call that succeeds."""
        errors = ['fake error 1', 'fake error 2']
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        table = MagicMock()
        rows = [dict(a=1), dict(b=2), dict(c=3)]
        client.get_table.return_value = table
        client.insert_rows.side_effect = [errors, errors, []]

        # HACK: It's a bit tricky to override the tenacity.retry decorator from a test.json. This seems to be the
        # simplest way to do it: replace the decorator's sleep with a mock.
        # See: https://stackoverflow.com/questions/47906671/python-retry-with-tenacity-disable-wait-for-unittest
        Bigquery.insert_rows.retry.sleep = MagicMock()

        self.assertEqual([], Bigquery.insert_rows(client, table_id, rows))
        client.insert_rows.assert_has_calls([call(table, rows)] * 3)

    def test_insert_rows__retry_on_error_ends_with_failure(self):
        """Simulate a persistent error, that ends with giving up."""
        errors = ['fake error 1', 'fake error 2']
        table_id = 'FAKE.TABLE'
        client = MagicMock()
        table = MagicMock()
        rows = [dict(a=1), dict(b=2), dict(c=3)]
        client.get_table.return_value = table
        client.insert_rows.return_value = errors

        # HACK: It's a bit tricky to override the tenacity.retry decorator from a test.json. This seems to be the
        # simplest way to do it: replace the decorator's sleep with a mock.
        # See: https://stackoverflow.com/questions/47906671/python-retry-with-tenacity-disable-wait-for-unittest
        Bigquery.insert_rows.retry.sleep = MagicMock()

        self.assertEqual(errors, Bigquery.insert_rows(client, table_id, rows))
        client.insert_rows.assert_has_calls([call(table, rows)] * INSERT_ROW_RETRY_MAX_RETRIES)


if __name__ == '__main__':
    unittest.main()





