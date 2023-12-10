import unittest
from unittest import mock

from functions import simulation_output
from google.cloud.bigquery.job import WriteDisposition


class SimulationOutputTest(unittest.TestCase):
    """Test the output row writers. It's a little tricky to test.json these, since they're all about I/O"""

    def test_serial_output_iterates_thru_writers(self):
        writer1 = mock.Mock()
        writer2 = mock.Mock()
        output_name = 'fake output'
        data = {'fake': 'data'}

        output = simulation_output.SerialOutput([writer1, writer2])
        writer1.lazy_initialize.assert_called_once()
        writer2.lazy_initialize.assert_called_once()

        output.send(output_name, data)
        writer1.write_rows.assert_called_once_with(output_name, data)
        writer2.write_rows.assert_called_once_with(output_name, data)

    def test_multiprocessing_output_writes_to_queue(self):
        queue = mock.Mock()
        writer1 = mock.Mock()
        writer2 = mock.Mock()
        output_name = 'fake output'
        data = {'fake': 'data'}

        output = simulation_output.MultiprocessingOutput(queue)
        output.send(output_name, data)
        writer1.assert_not_called()
        writer2.assert_not_called()
        queue.put.assert_called_once_with((output_name, data))

    def test_multiprocessing_output_stop_injects_signal(self):
        queue = mock.Mock()
        output = simulation_output.MultiprocessingOutput(queue)
        output.stop()
        queue.put.assert_called_once_with(simulation_output.MultiprocessingOutput.STOP_SIGNAL)

    def test_multiprocessing_output_processes_from_queue(self):
        """Tests that the processing is done, but does not test.json multiprocessing"""
        queue = mock.Mock()
        writer1 = mock.Mock()
        writer2 = mock.Mock()
        output_name1 = 'fake output'
        data1 = {'fake': 'data'}
        output_name2 = 'fake output 2'
        data2 = {'fake': 'data 2'}

        queue.get.side_effect = [
            (output_name1, data1),
            (output_name2, data2),
            simulation_output.MultiprocessingOutput.STOP_SIGNAL
        ]

        output = simulation_output.MultiprocessingOutput(queue)
        output.process_queue(queue, [writer1, writer2])

        writer1.lazy_initialize.assert_called_once()
        writer2.lazy_initialize.assert_called_once()

        writer1.write_rows.assert_has_calls([
            mock.call(output_name1, data1),
            mock.call(output_name2, data2)
        ])
        writer2.write_rows.assert_has_calls([
            mock.call(output_name1, data1),
            mock.call(output_name2, data2)
        ])

    def test_buffered_output_flushes_at_end_of_wrapped_scope(self):
        outputs = mock.Mock()
        row1 = {'fake': 'data1'}
        row2 = {'fake': 'data2'}
        row3 = {'fake': 'data3'}
        output1 = simulation_output.OUTPUT_PREMIUMS
        output2 = simulation_output.OUTPUT_LOSSES
        with simulation_output.BufferedOutputStream(outputs) as output_stream:
            output_stream.add(output1, [row1, row2])
            output_stream.add(output2, row3)
        outputs.send.assert_has_calls([
            mock.call(output1, [row1, row2]),
            mock.call(output2, [row3])
        ])

    def test_buffered_output_flushes_when_output_max_reached(self):
        outputs = mock.Mock()
        row1 = {'fake': 'data1'}
        row2 = {'fake': 'data2'}
        row3 = {'fake': 'data3'}
        row4 = {'fake': 'data4'}
        output1 = simulation_output.OUTPUT_PREMIUMS
        output2 = simulation_output.OUTPUT_LOSSES
        with simulation_output.BufferedOutputStream(
                outputs,
                per_output_max_rows=2) as output_stream:
            output_stream.add(output1, [row1])
            output_stream.add(output2, [row2, row3])
            # flush 1 expected.
            output_stream.add(output1, row4)
        # flush 2 expected.

        outputs.send.assert_has_calls([
            mock.call(output1, [row1]),  # flush 1
            mock.call(output2, [row2, row3]),  # flush 1
            mock.call(output1, [row4])  # flush2
        ])

    def test_buffered_output_flushes_when_buffer_max_reached(self):
        outputs = mock.Mock()
        row1 = {'fake': 'data1'}
        row2 = {'fake': 'data2'}
        row3 = {'fake': 'data3'}
        row4 = {'fake': 'data4'}
        output1 = simulation_output.OUTPUT_PREMIUMS
        output2 = simulation_output.OUTPUT_LOSSES
        with simulation_output.BufferedOutputStream(
                outputs,
                total_buffer_max_rows=2) as output_stream:
            output_stream.add(output1, [row1, row2])
            # flush 1 expected.
            output_stream.add(output2, [row3, row4])
            # flush 2 expected.
            output_stream.add(output1, row4)
        # flush 3 expected.

        outputs.send.assert_has_calls([
            mock.call(output1, [row1, row2]),  # flush 1
            mock.call(output2, [row3, row4]),  # flush 2
            mock.call(output1, [row4])  # flush 3
        ])

    @mock.patch('functions.simulation_output.os.path.exists')
    def test_csv_writer_raises_if_file_exists_no_overwite(self, os_path_exists):
        os_path_exists.return_value = True
        output_dir = '/fake/path'
        storm_type = 'fakestorm'
        try:
            _ = simulation_output.LocalCsvWriter(output_dir, storm_type, overwrite=False)
            self.fail('Expected exception for existing csv file')
        except simulation_output.RowWriter.InitializationError:
            pass

    @mock.patch('functions.simulation_output.os.path.exists')
    def test_csv_writer_ignores_if_file_exists_overwite_allowed(self, os_path_exists):
        output_dir = '/fake/path'
        storm_type = 'fakestorm'
        _ = simulation_output.LocalCsvWriter(output_dir, storm_type, overwrite=True)
        os_path_exists.assert_not_called()

    def test_csv_writer_builds_filepath(self):
        output_name = 'losses'
        output_dir = '/fake/path'
        storm_type = 'fakestorm'
        csv_writer = simulation_output.LocalCsvWriter(output_dir, storm_type, overwrite=True)
        self.assertEqual(f'{output_dir}/{storm_type}_{output_name}.csv', csv_writer.format_filepath(output_name))

    @mock.patch('functions.simulation_output.csv.DictWriter')
    @mock.patch('builtins.open')
    def test_csv_writer_get_writer_lazy_opens_file(self, file_open, dict_writer_init):
        filepath = '/fake/file/path'
        fieldnames = ['fake1', 'fake2']
        output_dir = '/fake/path'
        storm_type = 'fakestorm'
        dict_writer = mock.Mock()
        dict_writer_init.return_value = dict_writer

        csv_writer = simulation_output.LocalCsvWriter(output_dir, storm_type, overwrite=True)
        self.assertIs(dict_writer, csv_writer.get_writer(filepath, fieldnames))
        self.assertIs(dict_writer, csv_writer.get_writer(filepath, fieldnames))
        file_open.assert_called_once_with(filepath, 'w')
        dict_writer_init.assert_called_once_with(file_open.return_value, fieldnames)
        self.assertEqual(dict_writer, csv_writer.file_writers[filepath])

    @mock.patch('functions.simulation_output.LocalCsvWriter.get_writer')
    @mock.patch('functions.simulation_output.LocalCsvWriter.format_filepath')
    def test_csv_writer_write_row(self, format_filepath, get_writer):
        output_name = 'losses'
        data_key1 = 'fake key1'
        data_key2 = 'fake key2'
        data = {data_key1: 'fake1', data_key2: 'fake2'}
        output_dir = '/fake/path'
        storm_type = 'fakestorm'
        file_writer = mock.Mock()
        get_writer.return_value = file_writer

        csv_writer = simulation_output.LocalCsvWriter(output_dir, storm_type, overwrite=True)
        csv_writer.write_rows(output_name, data)
        format_filepath.assert_called_once_with(output_name)
        get_writer.assert_called_once_with(format_filepath.return_value, data.keys())
        file_writer.writerow.assert_called_once_with(data)

    @mock.patch('functions.simulation_output.requests.post')
    def test_webhook_writer_write_row_sends_request_post(self, requests_post):
        output_name = 'losses'
        url = 'fake_url'
        data = {'fake': 'data'}
        webhook_writer = simulation_output.WebhookWriter(url)
        webhook_writer.write_rows(output_name, data)
        requests_post.assert_called_once_with(url, json={'file_name': output_name, 'results': data})

    @mock.patch('functions.simulation_output.Bigquery.client')
    def test_bigquery_writer_lazy_initialize_creates_client(self, bq_client_init):
        project_id = 'fake-project'
        storm_type = 'hail'
        bq_writer = simulation_output.BigqueryRowWriter(project_id, storm_type)
        self.assertIsNone(bq_writer.client)
        bq_writer.lazy_initialize()
        self.assertIs(bq_client_init.return_value, bq_writer.client)

    @mock.patch('functions.simulation_output.BigqueryRowWriter.format_tablename')
    @mock.patch('functions.simulation_output.Bigquery.table_reference')
    @mock.patch('functions.simulation_output.Bigquery.insert_rows')
    def test_bigquery_writer_write_row_sends_insert(self, insert_rows, table_reference_func, format_tablename):
        project_id = 'fake-project'
        storm_type = 'hail'
        output_name = 'losses'
        tablename = 'fake-tablename'
        data = {'fake': 'data'}
        format_tablename.return_value = tablename
        bq_writer = simulation_output.BigqueryRowWriter(project_id, storm_type)
        bq_writer.client = mock.Mock()
        bq_writer.write_rows(output_name, data)
        insert_rows.assert_called_once_with(bq_writer.client, table_reference_func.return_value, data)
        table_reference_func.assert_called_once_with(tablename, project_id=project_id)

    def test_bigquery_writer_format_tablename(self):
        project_id = 'fake-project'
        storm_type = 'hail'
        output_name = 'losses'
        bq_writer = simulation_output.BigqueryRowWriter(project_id, storm_type)
        self.assertEqual(
            f'{bq_writer.DEFAULT_DATASET}.{storm_type}_{output_name}',
            bq_writer.format_tablename(output_name)
        )

    @mock.patch('functions.simulation_output.BigqueryRowWriter.format_tablename')
    @mock.patch('functions.simulation_output.Bigquery.table_reference')
    @mock.patch('functions.simulation_output.Bigquery.import_file')
    def test_bigquery_writer_upload_csv_file(self, import_file, table_reference_func, format_tablename):
        csv_filepath = 'fake-csv-filepath'
        project_id = 'fake-project'
        storm_type = 'hail'
        output_name = 'losses'
        tablename = 'fake-tablename'
        table_ref = mock.Mock()
        client = mock.Mock()
        format_tablename.return_value = tablename
        table_reference_func.return_value = table_ref
        bq_writer = simulation_output.BigqueryRowWriter(project_id, storm_type)
        bq_writer.client = client
        bq_writer.upload_csv_file(csv_filepath, output_name)
        import_file.assert_called_once_with(
            client, table_ref, csv_filepath,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True
        )

    def test_format_column_name_for_output(self):
        valid_col = 'NothingShouldBeReplaced'
        self.assertEqual(valid_col, simulation_output.format_column_name_for_output(valid_col))

        self.assertEqual(
            'prefixed_' + valid_col,
            simulation_output.format_column_name_for_output(valid_col, prefix='prefixed'))

        invalid_col = 'Replaced.with under-scores'
        self.assertEqual('Replaced_with_under_scores', simulation_output.format_column_name_for_output(invalid_col))

        custom_replacement = 'Custom.replacement'
        self.assertEqual(
            'Custom+replacement',
            simulation_output.format_column_name_for_output(custom_replacement, replace_invalid_with='+'))

    def test_upload_csv_to_bigquery__defaults_to_all_outputs(self):
        output_filepaths = {
            output_name: f'fake-filepath-{output_name}'
            for output_name in simulation_output.ALL_OUTPUTS
        }
        csv_writer = mock.Mock()
        bigquery_writer = mock.Mock()
        csv_writer.format_filepath.side_effect = list(output_filepaths.values())
        simulation_output.upload_csv_to_bigquery(csv_writer, bigquery_writer)
        csv_writer.format_filepath.assert_has_calls(
            [mock.call(output_name) for output_name in output_filepaths.keys()]
        )
        bigquery_writer.upload_csv_file.assert_has_calls(
            [mock.call(v, k) for k, v in output_filepaths.items()]
        )


    def test_upload_csv_to_bigquery(self):
        output_name = simulation_output.OUTPUT_PREMIUMS
        csv_writer = mock.Mock()
        bigquery_writer = mock.Mock()
        simulation_output.upload_csv_to_bigquery(csv_writer, bigquery_writer, output_names=[output_name])
        csv_writer.format_filepath.assert_called_once_with(output_name)
        bigquery_writer.upload_csv_file.assert_called_once_with(csv_writer.format_filepath.return_value, output_name)


    def test_bigquery_writer_format_tablename(self):
        project_id = 'fake-project'
        storm_type = 'hail'
        output_name = 'losses'
        bq_writer = simulation_output.BigqueryRowWriter(project_id, storm_type)
        self.assertEqual(
            f'{bq_writer.DEFAULT_DATASET}.{storm_type}_{output_name}',
            bq_writer.format_tablename(output_name)
        )

    @mock.patch('functions.simulation_output.BigqueryRowWriter.format_tablename')
    @mock.patch('functions.simulation_output.Bigquery.table_reference')
    @mock.patch('functions.simulation_output.Bigquery.import_file')
    def test_bigquery_writer_upload_csv_file(self, import_file, table_reference_func, format_tablename):
        csv_filepath = 'fake-csv-filepath'
        project_id = 'fake-project'
        storm_type = 'hail'
        output_name = 'losses'
        tablename = 'fake-tablename'
        table_ref = mock.Mock()
        client = mock.Mock()
        format_tablename.return_value = tablename
        table_reference_func.return_value = table_ref
        bq_writer = simulation_output.BigqueryRowWriter(project_id, storm_type)
        bq_writer.client = client
        bq_writer.upload_csv_file(csv_filepath, output_name)
        import_file.assert_called_once_with(
            client, table_ref, csv_filepath,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True
        )

    def test_format_column_name_for_output(self):
        valid_col = 'NothingShouldBeReplaced'
        self.assertEqual(valid_col, simulation_output.format_column_name_for_output(valid_col))

        self.assertEqual(
            'prefixed_' + valid_col,
            simulation_output.format_column_name_for_output(valid_col, prefix='prefixed'))

        invalid_col = 'Replaced.with under-scores'
        self.assertEqual('Replaced_with_under_scores', simulation_output.format_column_name_for_output(invalid_col))

        custom_replacement = 'Custom.replacement'
        self.assertEqual(
            'Custom+replacement',
            simulation_output.format_column_name_for_output(custom_replacement, replace_invalid_with='+'))

    def test_upload_csv_to_bigquery__defaults_to_all_outputs(self):
        output_filepaths = {
            output_name: f'fake-filepath-{output_name}'
            for output_name in simulation_output.ALL_OUTPUTS
        }
        csv_writer = mock.Mock()
        bigquery_writer = mock.Mock()
        csv_writer.format_filepath.side_effect = list(output_filepaths.values())
        simulation_output.upload_csv_to_bigquery(csv_writer, bigquery_writer)
        csv_writer.format_filepath.assert_has_calls(
            [mock.call(output_name) for output_name in output_filepaths.keys()]
        )
        bigquery_writer.upload_csv_file.assert_has_calls(
            [mock.call(v, k) for k, v in output_filepaths.items()]
        )

    def test_upload_csv_to_bigquery(self):
        output_name = simulation_output.OUTPUT_PREMIUMS
        csv_writer = mock.Mock()
        bigquery_writer = mock.Mock()
        simulation_output.upload_csv_to_bigquery(csv_writer, bigquery_writer, output_names=[output_name])
        csv_writer.format_filepath.assert_called_once_with(output_name)
        bigquery_writer.upload_csv_file.assert_called_once_with(csv_writer.format_filepath.return_value, output_name)


if __name__ == '__main__':
    unittest.main()
