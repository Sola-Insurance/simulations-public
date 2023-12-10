# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import argparse
import csv
import logging

from gcp.bigquery import Bigquery, BigqueryInsertStream
from google.cloud.bigquery import Client, SourceFormat, TableReference


IMPORT_DEFAULT_SKIP_LEADING_ROWS = 1
DEFAULT_NUM_ROWS_PER_STREAMING_WRITE = 1000


""" Writes data into a Bigquery table, either by file import or a streaming insert.

Usage:
    python gcp/tools/write_table.py [dataset].[tablename] /path/to/data.csv

Optional flags:
    --project [str]: The project id which houses the bigquery dataset. See note below.
    --format: The file format, must be one of bigquery.SourceFormat supported file types. Default is CSV.
    --import_skip_rows <N>: Skips the first N rows of the import file, Default=1
    --streaming: Reads the file one row at a time, and streams them into the bigquery. This could be useful if the file
            is exceptionally large, but really is meant as a demonstration of how to insert rows from live code.
    --debug: If set, sets logging level to DEBUG.
     

Note: 
To authenticate you need to do one of the following:
a) Run `cloud auth application-default login` and then use OAUTH in the browser to authenticate. This will auth you
as the Google user in your browser. But, the script may not be able to determine the project from this case. If so, 
you need to add the `--project` flag.

b) Set the `GOOGLE_APPLICATION_CREDENTIALS=path/to/service_credentials.json` for a downloaded credentials key file.  


Note: 
Assumes the dataset exists. To create one, I recommend the Web console: 
https://cloud.google.com/bigquery/docs/datasets#create-dataset

"""


def stream_local_file_to_table(client: Client,
                               table_ref: TableReference,
                               filepath: str):
    """Stream the contents of the given *local* CSV file into the bigquery.

    This is an alternative to using Bigquery's builtin file import functions. Why use this? It may be helpful if you
    have a very large file locally that you don't want to xfer into GCS and somehow doesn't work with Bigquery's
    `load_table_from_file` function. More realistically, you can use this as a sample of how to do a streaming
    insert into bigquery from live code as it generates output. IOW, use this when you don't want to output to a file.

    Note: To convert this over to read JSON data from a file, switch from csv.DictReader to doing a `json.loads()` on
    each row of the file.

    :param client: Client to the bigquery API.
    :param table_ref: The table to insert into
    :param filepath: Path to the local file to insert, CSV formatted.
    """

    with open(filepath, 'r') as f, \
         BigqueryInsertStream(client, table_ref) as bq_stream:

        reader = csv.DictReader(f)
        for row in reader:
            bq_stream.insert(row)


def main():
    parser = argparse.ArgumentParser('''
        Script to write rows into a bigquery table.
        There are a few ways to insert, including streaming a file from either local or GCS, or streaming inserts. 
        This script partially exists to demonstrate how to do the streaming insert. 
    ''')
    parser.add_argument('table_id', help='Qualified [dataset].[tablename] or [project].[dataset].[tablename] to '
                                         'create. Project name can be inferred from the authorized client, but '
                                         'depending how you auth (via gcloud login/env var) you may have to provide '
                                         'the --project flag')
    parser.add_argument('import_file', help='CSV file to import into the newly created table')
    parser.add_argument('--project', '-p', help='Specifies the project id to use when generating the client.')
    parser.add_argument('--format', '-f', default=SourceFormat.CSV, help='The format of the import file.' )
    parser.add_argument('--import_skip_rows', '-s', help='If importing, skip N rows',
                        default=IMPORT_DEFAULT_SKIP_LEADING_ROWS)
    parser.add_argument('--streaming', default=False, action='store_true',
                        help='If set, uses a streaming insert to upload the contents of the file. This is purely to ' 
                             'demonstrate how to do a streaming insert.')
    parser.add_argument('--debug', '-d', default=False, action='store_true',
                        help='When set, turn on DEBUG level logging.')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # Connect the Bigquery service.
    client = Bigquery.client(project_id=args.project)

    # Create a reference object to the table. This isn't strictly necessary for creating the table but helps
    # ensure the project-id is attached to the dataset.table.
    table_ref = Bigquery.table_reference(args.table_id, project_id=client.project)

    if args.streaming:
        try:
            stream_local_file_to_table(client, table_ref, args.import_file)
        except BigqueryInsertStream.Errors as e:
            logging.error(f'Failed to stream rows to table: {e.errors}')
    else:
        Bigquery.import_file(client, table_ref, args.import_file, skip_leading_rows=args.import_skip_rows)


if __name__ == '__main__':
    main()