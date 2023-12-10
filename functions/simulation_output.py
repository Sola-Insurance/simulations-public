# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import abc
import csv
import logging
import multiprocessing
import os
from collections import defaultdict

import requests

from functions import simulation_logging
from gcp.bigquery import Bigquery
from google.cloud import bigquery
from typing import Iterable, Union, Collection, List

"""Utility class for writing output from the generate_sample_X scripts into a few different output locations.

This module provides two pieces of functionality. 

1) A `RowWriter` abstract class that can be implemented to write a dict row of data to custom sink. To start we have
implementations of writing a local CSV file, sending to a webhook, and inserting to Bigquery. Because of 
multiprocessing, a RowWriter implementation has to be careful of when to initialize its inner-data. The init happens
in the main parent process and then each writer is shared with child processes via a proxy in multiprocesing.Manager.
Only pickle-able data is proxied. So, in order to initialize on the child-process size, RowWriter has a 
`lazy_initialize()` method that has to be called from the child_process.  

2) An 'OutputFanout` abstract class that can be implemented to fan-out a single dict row of data to 1..N different
`RowWriter`s. If we were just running everything in a single process, this class wouldn't be needed: a list would do.
But, when multiprocessing there may be multiple processes all simultaneously trying to write to a single output sink
which could produce bad results. So, to manage that potential contention we need an implementation of an `OutputFanout`
that can handle multiprocessing. (See `MultiProcessingOutput` for that).

We obscure most all of this through the abstract APIs of the writers and fanout.

Usage:
```
# Build a writer or writers for the hail simulation
storm_type = 'hail'
writers = [
  LocalCsvWriter(output_dir, storm_type),
  BigqueryWriter(project_id, storm_type)
]
```


```
# In a single process use-case, things are quite straightforward.
output = SerialOutput(writers)
data = dict(sim_id=sim_id, total=total, ...)
output.send(data)
```

```
# In multiprocessing, things are a little trickier in the setup.
#
# We use a Pool to create N processes to run the simulations.
import multiprocessing
output_queue = multiprocessing.Queue()
pool = multiprocessing.Pool(num_simulation_processes)

# Make the output writers available to all the simulation-processes. 
manager = multiprocessing.Manager()
manager.list(output_writers)

# Start a process to output from the queue, which will log at the INFO level.
output = MultiprocessingOutput(output_queue)
output_process = multiprocessing.Process(target=output.process_queue, args=(output_queue, writers, logging.INFO))
output_process.start()

# An example simulate can be called with its own process, receiving  the output object. 
def simulate(sim_id, output_queue):
    data = dict(sim_id=sim_id, total=total, ...)
    output.send(data)

# Run N=10 simulations.
list(pool.imap_unordered(functools.partial(simulate, output), range(num_simulation_processes))

# Remember to cleanup.
output.stop()
output_process.join()
pool.join()
```
"""

OUTPUT_EXPOSURES = 'exposures'
OUTPUT_PREMIUMS = 'premiums'
OUTPUT_LOSSES = 'losses'
OUTPUT_NLR = 'nlr'
ALL_OUTPUTS = [OUTPUT_EXPOSURES, OUTPUT_PREMIUMS, OUTPUT_LOSSES, OUTPUT_NLR]
INVALID_OUTPUT_COLUMN_CHARACTERS = [' ', '.', '-']
DEFAULT_COLUMN_REPLACEMENT_CHAR = '_'


class RowWriter(abc.ABC):
    """Abstract class for writing a single dict row to an output sink.

    Implements must provide the meat of `write_row`. An optional `lazy_intialize()` method must be called after
    initializing the writer and from within the process where writes will occur.
    """
    class InitializationError(Exception):
        """Raised if a writer cannot initialize itself."""
        pass

    def __init__(self, logger=None):
        self._logger = logger

    @property
    def logger(self):
        return self._logger or logging.getLogger()

    @logger.setter
    def logger(self, logger):
        self._logger = logger

    @abc.abstractmethod
    def write_rows(self, output_name: str, rows: Union[Iterable[dict], dict]):
        """Write the given dict or dicts to the appropriate output table/file/etc. based on the output_name."""
        pass

    def lazy_initialize(self):
        """Optional stage for the writer, called after init, from within the process that will perform writes,
            but before the first write occurs.

        When this is called may vary based on the type of `OutputFanout`.
        """
        pass


class OutputFanout(abc.ABC):
    """Abstract class for managing a set of RowWriters and sending row data to them for outputting."""
    @abc.abstractmethod
    def send(self, output_name: str, rows: Union[Iterable[dict], dict]):
        """Accept a row or rows for outputting. Eventual output is guaranteed, but may not be immediate."""
        pass


class SerialOutput(OutputFanout):
    """An inline, single-threaded, single-processed fanout, which calls each of its RowWriters serially."""
    def __init__(self, output_writers: Iterable[RowWriter]):
        super().__init__()
        self.output_writers = output_writers
        for writer in output_writers:
            writer.lazy_initialize()

    def send(self, output_name: str, rows: Union[Iterable[dict], dict]):
        """Immediately write the output row calling each RowWriter in turn."""
        for writer in self.output_writers:
            writer.write_rows(output_name, rows)


class MultiprocessingOutput(OutputFanout):
    """A fanout for use in a multiprocessing runtime.

    Processes can add rows to the queue with the expectation that a separate process will read the rows from the queue
    and pass them to its writers. See Usage at the top of the file.
    """
    STOP_SIGNAL = '__STOP_PROCESSING__'

    def __init__(self, output_queue: multiprocessing.Queue):
        super().__init__()
        self.output_queue = output_queue

    def send(self, output_name, rows: Union[Iterable[dict], dict]):
        """Place a row or rows on the output queue for processing."""
        self.output_queue.put((output_name, rows))

    def stop(self):
        """Inject a kill signal into the queue to stop the blocked process trying to read it."""
        self.output_queue.put(self.STOP_SIGNAL)

    @staticmethod
    def process_queue(output_queue: multiprocessing.Queue,
                      output_writers: Iterable[RowWriter],
                      logging_queue: multiprocessing.Queue = None,
                      log_level: Union[int, str] = None):
        """Run in a separate process! Read data from the queue and output it.

        Relies on data being a tuple of (output_name, data_type, data).
        Exception, if data is the string stop signal that breaks out of the queue consumption.
        """
        # A little bit of a hack, logs from the output process will be labeled with sim_id:-1.
        logger = simulation_logging.get_logger(-1, logging_queue, log_level=log_level)
        for writer in output_writers:
            if not writer.logger:
                writer.logger = logger
            writer.lazy_initialize()

        logger.info('Starting loop to process output rows.')
        while True:
            message = output_queue.get()
            if isinstance(message, str) and message == MultiprocessingOutput.STOP_SIGNAL:
                break

            (output_name, rows) = message
            for output_writer in output_writers:
                output_writer.write_rows(output_name, rows)

        logger.info('Output processing complete.')

class BufferedOutputStream:
    """For outputting many rows through the outputs, buffers rows before flushing them in one big write.

    With the switch to the v2 schema, we're writting many more rows. As a result, IO is now important. Writing one
    row at a time may carry overhead.

    This is a decorator for buffer output rows for each output.
    Usage:
    ```
        def run_sim(outputs, ...):
            with BufferedOutputStream(outputs) as output_stream:
                for ...:
                    # Loop that produces lots of rows
                    output_stream.add(output_name, rows)
            # At this point all rows will be automatically flushed to their outputs.

            # This is also permitted:
            output_stream = BufferedOutputStream(outputs)
            for ...:
                # Loop that produces lots of rows
                output_stream.add(output_name, rows)
            output_stream.flush()
    ```

    Note: I (greg h) don't love having to do this. This only exists because we (and by that I mean I) abstracted away
    the knowledge in the simulation code of what outputs we're writing to. Potentially, we should get rid of the
    OutputFanout class and just pass each writer around. Then we could use things like the BigqueryInsertStream.
    """
    DEFAULT_PER_OUTPUT_MAX_ROWS = 500
    DEFAULT_TOTAL_BUFFER_MAX_ROWS = 5000

    def __init__(self,
                 outputs: OutputFanout,
                 per_output_max_rows: int = DEFAULT_PER_OUTPUT_MAX_ROWS,
                 total_buffer_max_rows: int = DEFAULT_TOTAL_BUFFER_MAX_ROWS,
                 logger: logging.Logger = None):
        """Initialize, but intended use is as a wrapper.

        :param outputs: The OutputFanout which maintains the different forms of RowWriters.
        :param per_output_max_rows: Max number of rows in any output's buffer before ALL are flushed.
        :param total_buffer_max_rows: Max number of total rows across all outputs before ALL are flushed.
        :param logger: Logger, useful if we're in a multiprocessing environment.
        """
        self.outputs = outputs
        self.per_output_max_rows = per_output_max_rows
        self.total_buffer_max_rows = total_buffer_max_rows
        self.logger = logger or logging.getLogger()
        self.total_in_buffer = 0
        self._init_buffer()

    def __enter__(self):
        self._init_buffer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Flush the outputs left in the buffers at the end of scope.
        self.flush()

    def add(self, output_name: str, rows: Union[Iterable[dict], dict]):
        if isinstance(rows, dict):
            rows = [rows]
        self.buffer[output_name].extend(rows)
        self.total_in_buffer += len(rows)

        if (len(self.buffer[output_name]) >= self.per_output_max_rows
                or self.total_in_buffer >= self.total_buffer_max_rows):
            self.flush()

    def flush(self):
        for output_name, rows in self.buffer.items():
            logging.debug(f'Flushing ({len(rows)}) to output({output_name})')
            if rows:
                self.outputs.send(output_name, rows)
        self._init_buffer()

    def _init_buffer(self):
        self.buffer = defaultdict(list)
        self.total_in_buffer = 0


class LocalCsvWriter(RowWriter):
    """RowWriter which writes rows to CSV files on the local filesystem.

    Note: to minimize file I/O, we keep each file open until the end of the run.
    """
    OUTPUTS_TO_FILENAME_FMT = '{output_dir}{slash}{storm_type}_{output_name}.csv'

    def __init__(self, output_dir: str, storm_type: str, overwrite=False):
        """
        :param output_dir: Where output CSV files will be written.
        :param storm_type: The type of storms being simulated.
        :param overwrite: Boolean, if False, tries to avoid overwriting an existing file. Note, this only checks the
            known output names at initialization.
        """
        super().__init__()
        self.output_dir = output_dir
        self.storm_type = storm_type
        self.file_writers = {}
        self.overwrite = overwrite

        if not overwrite:
            self._enforce_output_files_do_not_exist()

    def _enforce_output_files_do_not_exist(self):
        # Check if the potential output files exist and if we're allowed to overwrite them.
        # Note this doesn't protect against all possible output_files if the caller passes an unknown output_name.
        for output_name in ALL_OUTPUTS:
            filepath = self.format_filepath(output_name)
            if os.path.exists(filepath):
                raise RowWriter.InitializationError(f'File exists and overwrite is not enabled: {filepath}')

    def lazy_initialize(self):
        """ Make sure the output directories exist."""
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

    def format_filepath(self, output_name: str) -> str:
        """Generate an absolute filepath based on the output_name."""
        return LocalCsvWriter.OUTPUTS_TO_FILENAME_FMT.format(
            output_dir=self.output_dir,
            slash='/' if not self.output_dir.endswith('/') else '',
            storm_type=self.storm_type,
            output_name=output_name
        )

    def get_writer(self, filepath: str, fieldnames: Collection[str]) -> csv.DictWriter:
        """Lazy-open an output file, wrapping it in a DictWriter. The open file is kept, keyed by filepath.

        :param filepath: Complete path to the output file to open
        :param fieldnames: Iterable of the fieldnames (aka columns) that will be written to the file. This is only
            used the first time the file is opened.
        :return: The DictWriter with which to write CSV rows to the file.
        """
        file_writer = self.file_writers.get(filepath)
        if not file_writer:
            logging.debug(f'Opening file: {filepath}')
            filehandle = open(filepath, 'w')
            file_writer = csv.DictWriter(filehandle, fieldnames)
            self.file_writers[filepath] = file_writer
        return file_writer

    def write_rows(self, output_name, rows: Union[Iterable[dict], dict]):
        """Write the given dict or dicts of row data for the given output_name output."""
        filepath = self.format_filepath(output_name)

        if isinstance(rows, dict):
            rows = [rows]

        for row in rows:
            file_writer = self.get_writer(filepath, row.keys())
            file_writer.writerow(row)


class WebhookWriter(RowWriter):
    """RowWriter which sends its output rows to a wbhook."""
    DEFAULT_URL = 'https://solainsurance.ngrok.io/webhooks/upload'

    def __init__(self, url: str = DEFAULT_URL):
        super().__init__()
        self.url = url

    def write_rows(self, output_name, rows: Union[Iterable[dict], dict]):
        """Send the given dict of row data for the specified output to the webhook."""
        body = {
            "file_name": output_name,
            "results": rows
        }
        requests.post(self.url, json=body)


class BigqueryRowWriter(RowWriter):
    """RowWriter which inserts an output row into a Bigquery Table.

    Internally, this class creates a bigquery client. Google advises creating the client *after* os.fork has occurred,
    so it is instantiated in the lazy_initialize.

    Example doc: https://cloud.google.com/python/docs/reference/lifesciences/latest/multiprocessing
    """
    DEFAULT_DATASET = 'simulations_v2'
    OUTPUT_TABLENAME_FMT = '{dataset}.{storm_type}_{output_name}'

    def __init__(self, project_id: str, storm_type: str, dataset: str = DEFAULT_DATASET):
        """
        :param project_id: The GCP project in which the bigquery table lives.
        :param storm_type: The type of storm being simulated.
        :param dataset: The dataset container for the bigquery table.
        """
        super().__init__()
        self.project_id = project_id
        self.storm_type = storm_type
        self.dataset = dataset
        self.client = None  # Wait to create the client until lazy_initialize, in case we're multiprocessing.

    def lazy_initialize(self):
        """Create the bigquery client."""
        self.client = Bigquery.client(self.project_id)

    def format_tablename(self, output_name: str) -> str:
        return BigqueryRowWriter.OUTPUT_TABLENAME_FMT.format(
            dataset=self.dataset, storm_type=self.storm_type, output_name=output_name)

    def write_rows(self, output_name, rows: Union[Iterable[dict], dict]):
        """Insert the given dict row into the bigiquery."""
        tablename = self.format_tablename(output_name)
        table_ref = Bigquery.table_reference(tablename, project_id=self.project_id)
        Bigquery.insert_rows(self.client, table_ref, rows)

    def upload_csv_file(self, csv_filepath, output_name):
        tablename = self.format_tablename(output_name)
        table_ref = Bigquery.table_reference(tablename, project_id=self.project_id)
        Bigquery.import_file(self.client, table_ref, csv_filepath,
                             # Overwrite the table if it already exists.
                             write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
                             # Allow there to be more columns than expected.
                             ignore_unknown_values=True)


def format_column_name_for_output(
        column_name: str, prefix: str = None, replace_invalid_with: str = DEFAULT_COLUMN_REPLACEMENT_CHAR) -> str:
    """Given a column name, remove any invalid characters for output and attach the prefix, if provided."""
    for invalid_char in INVALID_OUTPUT_COLUMN_CHARACTERS:
        column_name = column_name.replace(invalid_char, replace_invalid_with)

    # To support columns named something like {state}_{zipcode}.
    if prefix:
        column_name = f'{prefix}_{column_name}'
    return column_name


def upload_csv_to_bigquery(
        csv_writer: LocalCsvWriter, bigquery_writer: BigqueryRowWriter, output_names: List[str] = None):
    """Use the given CSV and Bigquery Writers to upload CSV output files into Bigquery tables.

     Note: This is destructive. It OVERWRITES the Bigquery table.

     :param csv_writer: LocalCsvWriter instance for simulation output to CSV files.
     :param bigquery_writer: BigqueryRowWriter instance for simulation output. Note, we use a 'row' writer, but in this
        case we're not actually writing any rows.
     :param output_names: List of outputs to upload. If None, uploads all known outputs.
     """
    output_names = output_names or ALL_OUTPUTS
    for output_name in output_names:
        csv_filepath = csv_writer.format_filepath(output_name)
        bigquery_writer.upload_csv_file(csv_filepath, output_name)
