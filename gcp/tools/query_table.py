# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import argparse

from gcp.bigquery import Bigquery

"""Example script for running a query to read rows from a Bigquery table.

Usage:
# Select all from a table
python bigquery/tools/query_table.py <project id> --table <dataset>.<tablename>

# Use a limit and/or an offset.
--limit x
--offset y

# Add a where clause
--where "<column>=<value>"

# Run your own query
python bigquery/tools/query_table.py <project id> --query "<query>"

Note:
Remember, export GOOGLE_APPLICATION_CREDENTIALS=credentials_file.json if you want to use a key from a service account.
Else, the script will attempt to run as 'you'.

If you get this error:
```
  google.api_core.exceptions.BadRequest: 400 POST ... ProjectId and DatasetId must be non-empty
```
then the project_id or dataset could not be resolved. Check your command line for typos.
"""

DEFAULT_QUERY_SELECT_FMT = 'SELECT {columns} FROM {table}'
COLUMNS_DEFAULT = '*'
WHERE_FMT = ' WHERE {where} '
LIMIT_FMT = ' LIMIT {limit} '
OFFSET_FMT = ' OFFSET {offset} '


def build_query(table_ref, columns, where, limit, offset):
    """Build a query string from the given parameters."""
    query = DEFAULT_QUERY_SELECT_FMT.format(
        columns=columns or COLUMNS_DEFAULT,
        table=table_ref
    )

    if where:
        query += WHERE_FMT.format(where=where)

    if limit:
        query += LIMIT_FMT.format(limit=limit)

    if offset:
        query += OFFSET_FMT.format(offset=offset)
    return query


def execute_query(client, query):
    """Executes the given query.

    :returns: The result iterator directly from Bigquery. You can cast each row as a dict().
    """
    query_job = client.query(query)
    return query_job.result()


def main():
    parser = argparse.ArgumentParser('''
        A silly little script for querying from a Bigquery. By default has a built-in select, and you can pass in 
        columns, where, limit, offset clauses. Or use the --query flag to run your own query.
    ''')
    parser.add_argument('project_id', help='The project which hosts the bigquery table.')
    parser.add_argument('--columns', '-c',
                        help='Comma-separated list of columns to query. Example "col1,col2,col3"',
                        default=None)
    parser.add_argument('--table', '-t',
                        help='Table to query from. If not using --query, this is required.',
                        default=None)
    parser.add_argument('--where', '-w',
                        help='Where clause to add to the query. Example "column=value"',
                        default=None)
    parser.add_argument('--limit', '-l',
                        type=int,
                        help='Max rows to read in query.',
                        default=None)
    parser.add_argument('--offset', '-o',
                        type=int,
                        help='Skip the first N rows',
                        default=None)
    parser.add_argument('--query', '-q',
                        help='Override the parameterized built-in query, and just run this query as is.',
                        default=None)
    parser.add_argument('--no_execute', '-n',
                        help='If set, print the query but don\t execute it.',
                        action='store_true',
                        default=False)
    args = parser.parse_args()

    if args.query:
        query = args.query
    else:
        if not args.table:
            raise Exception('Table is required when generating a query. See --table.')
        table_ref = Bigquery.table_reference(args.table, project_id=args.project_id)
        query = build_query(table_ref, args.columns, args.where, args.limit, args.offset)

    client = Bigquery.client(project_id=args.project_id)

    print(f'Running query: {query}')
    if args.no_execute:
        results = ['*** Skipped execution ***']
    else:
        results = execute_query(client, query)
    print('Results from query')
    print('------------------')
    for i, row in enumerate(results):
        print(f'[{i}]  {dict(row)}')
    print('------------------')


if __name__ == '__main__':
    main()
