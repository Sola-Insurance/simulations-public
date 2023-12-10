# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import logging
import tenacity

from google.cloud import bigquery
from typing import Any, Union, List, Sequence

GCS_FILE_URL_PREFIX = 'gs://'
IMPORT_FILE_DEFAULT_FORMAT = bigquery.SourceFormat.CSV
INSERT_ROW_RETRY_MAX_RETRIES = 5


class Bigquery:
    """Wrapper class around the APIs to Google Bigquery.

        Usage:
        ```
            # Generate a Google API client. ou can then either use that client's APIs directly, or pass it to the
            # helper functions in this file.
            project_id = ...
            client = Bigquery.client(project_id=project_id)  # Optionally, pass credentials too.

            # Validate a table's name, connect it to the current project.
            table_id = 'mydataset.mytable'
            table_ref = client.table_reference(table_id, project_id=project_id)

            # Insert data from a file into the table.
            Bigquery.import_file(client, table_ref, filepath)
        ```
    """

    @staticmethod
    def client(project_id: str = None, credentials: Any = None) -> bigquery.Client:
        """Creates and returns a new client for accessing the Bigquery service.

        See:
        https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client

        :param credentials: An instance of Google Cloud credentials, to authenticate the client. If not provided, the
        bigquery client may instantiate its own credentials, inferred from the environemnt.
        :param project_id: Name of the project containing the Bigquery table. If not provided, the project is inferred
        table the environment.
        :returns: A Bigquery Client.
        """
        return bigquery.Client(project=project_id, credentials=credentials)

    @staticmethod
    def table_reference(table_id: str, project_id: str = None) -> bigquery.TableReference:
        """Builds a complete bigquery.TableReference around the given table id and optional project.

        A Bigquery tableid str can come in the format of [dataset].[tablename] or [project].[dataset].[tablename].
        Without the project_id, the underlying Google code attempts to infer it from the environment. By
        building the TableReference we can force a validation, potentially raising a ValueError. If the TableReference
        is returned, we know that the project_id can be determined and that the tableid is complete and usable. It
        doesn't guarantee that the table actually exists, only that the naming is valid.

        :param table_id: String in form of [dataset].[tablename] or [project].[dataset].[tablename]
        :param project_id: Name of the project containing the Bigquery table. If not provided, the project is inferred
        table the environment.
        :returns: A Bigquery TableReference which contains the validated table info. The table isn't guaranteed to
        exist. It's only a reference to a valid tablename.
        """
        return bigquery.TableReference.from_string(table_id, default_project=project_id)

    @staticmethod
    def import_file(client: bigquery.Client,
                    table: Union[bigquery.Table, bigquery.TableReference, str],
                    import_filepath: str,
                    source_format: bigquery.SourceFormat = IMPORT_FILE_DEFAULT_FORMAT,
                    schema: str = None,
                    skip_leading_rows: int = None,
                    write_disposition: bigquery.job.WriteDisposition = None,
                    ignore_unknown_values: bool = None):
        """Insert the data from the given file uri into the given bigquery table.

        Bigquery can read from either GCS or local filesystem, the function signatures for each are slightly different.
        This is just a wrapper around both to provide an example of importing either a local file or a cloud-storage
        file.

        Note: This function waits until the entire file is imported. If you wish to perform the load async, it could be
        modified to return the load_job.

        :param client: The Bigquery client, for connecting to the cloud.
        :param table: bigquery.Table object representing an existing table.
        :param import_filepath: Path to the CSV file, either on local FS or GCS, to read into the table
        :param source_format: Optional, format of file type to read, default is CSV.
        :param schema: Optional, one of bigquery.SourceFormat, the file format to read. If None, the import may still
            succeed if the file schema exactly matches to the table's.
        :param skip_leading_rows: Optional, skip the first N rows of the file.
        :param write_disposition: Optional, policy to use if the table already exists.
        :param ignore_unknown_values: Optional, if set, skips columns in the import rows which don't correspond to
            columns in the destination table.
        """
        if not isinstance(table, bigquery.Table) and not schema:
            table = client.get_table(table)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=source_format
        )

        if skip_leading_rows:
            job_config.skip_leading_rows = skip_leading_rows

        if write_disposition:
            job_config.write_disposition = write_disposition

        if ignore_unknown_values is not None:
            job_config.ignore_unknown_values = ignore_unknown_values

        if import_filepath.startswith(GCS_FILE_URL_PREFIX):
            logging.info(f'Importing GCS file {import_filepath} to {table}')
            load_job = client.load_table_from_uri(import_filepath, table, job_config=job_config)
        else:
            logging.info(f'Importing local file: {import_filepath} to {table}')
            with open(import_filepath, 'rb') as file_obj:
                load_job = client.load_table_from_file(file_obj, table, job_config=job_config)

        logging.debug('Waiting for import to complete')
        load_job.result()

    @staticmethod
    @tenacity.retry(
        retry=tenacity.retry_if_result(lambda e: e),  # Retries if the result evaluates to True.
        retry_error_callback=lambda state: state.outcome.result(),  # If fail, return the errors from the last attempt.
        stop=tenacity.stop_after_attempt(INSERT_ROW_RETRY_MAX_RETRIES),
        wait=tenacity.wait_random_exponential(multiplier=1, max=60)
    )
    def insert_rows(client: bigquery.Client,
                    table: Union[bigquery.Table, bigquery.TableReference, str],
                    rows: List[dict],
                    **kwargs) -> Sequence[dict]:
        """Performs a streaming insert into the given table, retrying if errors are returned.

        Note, the Bigquery API has a 10MB size limit to its inserts and may reject large writes.

        TODO: Determine if the errors are transient or permanent.
        TODO: Consider adding retry around specific exceptions raised by the client.
        TODO: Consider fail-over cases where the batch is written to a failure table/file for processing later.
        TODO: It may be worth implementing a separate wrapper around the API `client.insert_rows_json` for JSON data.

        :param client: The Bigquery client, for connecting to the cloud.
        :param table: bigquery.Table object representing an existing table.
        :param rows: List of dict containing new rows to insert.
        :param kwargs: K=V args to pass to the bigquery client `insert_rows()` call.
        :return: A Sequence of errors, empty if no error.
        """
        if type(table) in [bigquery.TableReference, str]:
            # When using a TableReference, the internal client.insert_rows requires the schema to be passed.
            # To avoid that, lookup the table object.
            # Note: this may slow repeated inserts, so it may be worth calling `client.get_table()` first at passing
            #    its result in instead of a TableReference.
            table = client.get_table(table)

        errors = client.insert_rows(table, rows, **kwargs)
        if errors:
            logging.warning(f'Insert of ({len(rows)}) rows into table ({table}) failed: {errors}')
        else:
            logging.debug(f'Successfully inserted ({len(rows)}) rows into table ({table})')
        return errors


class BigqueryInsertStream:
    """
        A Contextmanager that wraps streaming inserts into a bigquery with batched writes.

        The underlying Google Bigquery code rejects writes that exceed 10MB, so for large volume writes we need to
        batch them into large, but not too large writes. The easiest way to do this is to limit batch-size by number
        of rows but a more advanced version would look at byte.

        The API of this class is a call to `insert(rows)` but the internals will only execute the insert when the
        total number of rows exceeds the batch size or the context exits. If the caller wants to force a write, they
        can call `flush()`.

        The `flush()` call to Bigquery has retry logic built-in but this may take some fine-tuning based on errors seen
        -- some may be transient and others permanent. See the TODOs on the `flush()` method.

        Usage:
        ```
        try:
            with BigqueryInsertStream(client, table) as stream:
                ... some logic that generates 1 to many rows ...
                stream.insert(rows)

                # Optional, force the stream to write the rows it has batched so far.
                stream.flush()

                ... more logic ...
                stream.insert(rows)
            # By the time the context exits, all rows will be flushed.
        except BigqueryInsertStream.Errors e:
            logging.errors(f'Failed to insert rows with errors: {e.errors})
        ```
    """
    class Errors(Exception):
        """Raised if an insert failed, propagating the last set of errors returned by the API."""

        def __init__(self, errors):
            self.errors = errors

    DEFAULT_STREAMING_INSERT_BATCH_SIZE = 10000
    batch = None
    total = 0

    def __init__(self,
                 client: bigquery.Client,
                 table: Union[bigquery.Table, bigquery.TableReference, str],
                 batch_size: int = DEFAULT_STREAMING_INSERT_BATCH_SIZE):
        if batch_size <= 0:
            raise Exception('Invalid batch size, must be greater than zero.')

        self.client = client
        self.table = table
        self.batch_size = batch_size

    def __enter__(self):
        self.batch = []
        self.total = 0
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        errors = self.flush()
        if errors:
            raise BigqueryInsertStream.Errors(errors)
        logging.info(f'BigqueryInputStream successfully wrote ({self.total}) rows')

    def insert(self, rows: Union[List[dict], dict]):
        if isinstance(rows, list):
            self.batch.extend(rows)
        else:
            self.batch.append(rows)

        logging.debug(f'Bigquery insert is now ({len(self.batch)}) rows')
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):
        """Attempts to flush the batched rows into the Bigquery Table. This function has a decorator that retries if
        the streaming insert returns errors, on the assumption that they may be transient. If they still fail, it
        immediately raises an exception to halt the streaming.

        :raises BigqueryInsertError: Raised if the bigquery insert returns errors for any reason.
        """
        if self.batch:
            logging.debug(f'Flushing ({len(self.batch)}) rows to bigquery: {self.table}')
            errors = Bigquery.insert_rows(self.client, self.table, self.batch)
            if errors:
                raise BigqueryInsertStream.Errors(errors)
            self.total += len(self.batch)
            self.batch = []
