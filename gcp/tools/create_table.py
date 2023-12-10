# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import argparse
import logging
from typing import Optional

from gcp.bigquery import Bigquery
from google.api_core.exceptions import ClientError
from google.cloud import bigquery


""" Creates a Bigquery table, optionally, populating it with data.

Usage:
    python gcp/tools/create_table.py [dataset].[tablename] /path/to/schema.json
    
Optional flags:
    --project [str]: The project id which houses the bigquery dataset. See note below.
    --import_file [filename]: Loads the CSV file into the bigquery table. Accepts both a gs:// filepath in GCS, or 
            a local filesystem path.
    --import_skip_rows <N>: Skips the first N rows of the import file, Default=1
    --ok_if_exists: If the table create fails because the table already exists, continue on to the import. 


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

IMPORT_DEFAULT_SKIP_LEADING_ROWS = 1
MILLISECS_IN_30_DAYS = 1000 * 86400 * 30


def create_table(client: bigquery.Client, table_ref: bigquery.TableReference, schema_filepath: str,
                 partitioning: Optional[bigquery.TimePartitioning],
                 ok_if_exists: bool = False) -> bigquery.Table:
    """Creates a Bigquery table with the specified schema.

    :param client: The Bigquery client, for connecting to the cloud.
    :param table_ref: bigquery.TableReference object, representing the table to create.
    :param schema_filepath: Path to local JSON file that contains the schema to give the created table.
    :param partitioning: Optional TimePartitioning for the Bigquery table.
    :param ok_if_exists: If True, will swallow a CONFLICT error and fetch the already-existing table.
    :return: The instantiated bigquery.Table object.
    """
    print(f'Using schema from [{schema_filepath}]')
    schema = client.schema_from_json(schema_filepath)
    table = bigquery.Table(table_ref, schema=schema)

    if partitioning:
        table.time_partitioning = partitioning

    try:
        table = client.create_table(table)
    except ClientError as e:
        if e.code == 409 and ok_if_exists:
            print(f'Warning: Ignoring error bc ok_if_exists=True: {e}')
            table = client.get_table(table_ref)
        else:
            raise

    return table


def main():
    parser = argparse.ArgumentParser('Script to read a CSV file into a bigquery table')
    parser.add_argument('table_id', help='Qualified [dataset].[tablename] or [project].[dataset].[tablename] to '
                                         'create. Project name can be inferred from the authorized client, but '
                                         'depending how you auth (via gcloud login/env var) you may have to provide '
                                         'the --project flag')
    parser.add_argument('schema_file', help='File containing the JSON formatted schema for the new table')
    parser.add_argument('--project', '-p', help='Specifies the project id to use when generating the client.')

    partitioning_group = parser.add_argument_group('Partitioning')
    partitioning_group.add_argument('--partition',
                                    help='If set, applies partitioning to the table.',
                                    action='store_true',
                                    default=False)
    partitioning_group.add_argument('--partition_column',
                                    help='Specific a column name to use for partitioning. '
                                         'If not set, uses insertion timestamp.',
                                    default=None)
    partitioning_group.add_argument('--partition_type',
                                    help='Time granularity for partitioning. If unset, uses DAY',
                                    default=None)
    partitioning_group.add_argument('--partition_expiration_ms',
                                    help='Time in milliseconds to expire table partitions. '
                                         'Data will be deleted this delta after insertion.',
                                    default=MILLISECS_IN_30_DAYS)

    import_group = parser.add_argument_group('Import Data')
    import_group.add_argument('--import_file', '-i', help='CSV file to import into the newly created table')
    import_group.add_argument('--import_format', '-f', default=bigquery.SourceFormat.CSV,
                              help='The format of the import file.' )
    import_group.add_argument('--import_skip_rows', '-s', help='If importing, skip N rows',
                              default=IMPORT_DEFAULT_SKIP_LEADING_ROWS)

    parser.add_argument('--ok_if_exists', default=False, action='store_true',
                        help='If set, allows the table to already exist in order to perform an import.')
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
    logging.info(f'Creating table: [{table_ref}]')

    partitioning = None
    if args.partition:
        partitioning = bigquery.TimePartitioning(
            type_=args.partition_type,
            field=args.partition_column,
            expiration_ms=args.partition_expiration_ms
        )
        logging.info(f'Partitioning table with {partitioning}')

    table = create_table(client, table_ref, args.schema_file,
                         partitioning=partitioning,
                         ok_if_exists=args.ok_if_exists)
    logging.info(f'Created BQ table: {table}')

    if args.import_file:
        Bigquery.import_file(client, table, args.import_file,
                             source_format=args.import_format,
                             schema=table.schema,
                             skip_leading_rows=args.import_skip_rows)


if __name__ == '__main__':
    main()