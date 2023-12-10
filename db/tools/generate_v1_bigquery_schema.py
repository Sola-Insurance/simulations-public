# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import argparse
import os

"""Script to help bridge from the original CSV output to a Bigquery output. 

Usage:
```
  # First generate the CSV headers, output to files/hail_output
  python functions/populate_headers.py
  
  # Generate a Bigquery schema based on each CSV header file, output to db/schema/v1
  python db/tools/generate_v1_bigquery_schema.py
```

Optional Flags:
--output_dir: Where to write the schema files, default is db/schema/v1
--table: One of [exposures, losses, nlr, premium]

Note: 
Bigquery column names don't allow a '.'. This script replaces it and several other chars with a '_'.
"""

DEFAULT_INPUT_DIR = 'files/hail_output'
DEFAULT_OUTPUT_DIR = 'db/schema/v1'
HEADER_FILENAME_FMT = '{input_dir}/{table}.csv'
SCHEMA_FILENAME_FMT = '{output_dir}/{table}_schema.json'

TABLES = ['exposures', 'losses', 'nlr', 'premium']
DEFAULT_COLUMN_TYPE = 'FLOAT64'
CUSTOM_COLUMN_TYPES = {
    'SimID': 'INT64'
}
REPLACEMENT_CHARACTERS = {
    '.': '_',
    ' ': '_',
    '\'': '_',
}
SCHEMA_COLUMN_TEMPLATE = '{{ "name": "{column_name}", "type": "{column_type}", "mode": "NULLABLE" }}'


def generate_schema(input_dir, output_dir, table, overwrite=False):
    """ Read the header CSV file for the given table and generate a bigquery schema file

    The header files are assumed to be single-line, comma-separated.

    :param input_dir: Directory from where to read the header CSV file.
    :param output_dir: Directory to write the bigquery schema file
    :param table: Table name.
    :param overwrite: If true, overwrite an existing file. If false, and file exists, raise exception.
    """
    input_file = HEADER_FILENAME_FMT.format(input_dir=input_dir, table=table)
    output_file = SCHEMA_FILENAME_FMT.format(output_dir=output_dir, table=table)
    if not os.path.exists(input_file):
        raise Exception(f'Table header file does not exist: {input_file}')

    if not overwrite and os.path.exists(output_file):
        raise Exception(f'Output file exists, and --overwrite was not set: {output_file}')

    columns = []
    with open(input_file, 'r') as infile:
        headers = [col.strip() for col in infile.read().strip().split(',')]

        for header in headers:
            if not header:
                # There are some empty columns in the CSV files.
                continue

            for invalid_char, replacement in REPLACEMENT_CHARACTERS.items():
                if invalid_char in header:
                    # The header contains an invalid character for a Bigquery column name.
                    header = header.replace(invalid_char, replacement)

            column_type = CUSTOM_COLUMN_TYPES.get(header, DEFAULT_COLUMN_TYPE)
            columns.append(SCHEMA_COLUMN_TEMPLATE.format(column_name=header, column_type=column_type))

    with open(output_file, 'w') as outfile:
        outfile.write('[\n')
        outfile.write(',\n'.join([f'    {column}' for column in columns]))
        outfile.write('\n]')


def main():
    parser = argparse.ArgumentParser('Script for converting the v1 CSV schema to a Bigquery schema.')
    parser.add_argument('--input_dir', '-i', help='Where to find the CSV header files', default=DEFAULT_INPUT_DIR)
    parser.add_argument('--output_dir', '-o', help='Where to write schema files', default=DEFAULT_OUTPUT_DIR)
    parser.add_argument('--table', '-t', nargs='*',
                        help='Table(s) to generate schema for, space separated',
                        default=TABLES)
    parser.add_argument('--overwrite',
                        help='If the bigquery schema file already exists, overwrite it',
                        default=False,
                        action='store_true')
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        print('** ERROR ** ')
        print(f'Output dir does not exist: {args.output_dir}')

    print(f'Generating schema for [{args.table}] ==> {args.output_dir}')
    for table in args.table:
        generate_schema(args.input_dir, args.output_dir, table, overwrite=args.overwrite)


if __name__ == '__main__':
    main()
