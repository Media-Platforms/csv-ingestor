# csv-ingestor

Load data from CSV files into PostgreSQL tables.

## Installation

`pip install csv-ingestor`

## Examples

In the simplest case, load data from a CSV file into a table:

    from csv_ingestor import Ingestor, ingest_file

    class MyIngestor(Ingestor):
        filename_pattern = r'simple\.\d{8}_\d{4}\.csv(\.gz)?'
        tables = [
            {
                'table': 'my_table',
                'csv_columns': ('id', 'value'),
            }
        ]

    ingest_file('simple.20240910_1430.csv.gz')

But maybe you have multiple tables to load from different CSV files, or from different fields in
each file, and the column names don't match what's in the CSV files, and the data isn't quite the right shape either, and you'd like to skip some CSV records, and you'd like to update existing DB
records:

    from csv_ingestor import CSVPicker, Ingestor, SkipRecord, ingest_file

    class MyPicker(CSVPicker):

        def check_skip(self, record):
            if record['value'].startswith('SKIP!'):
                raise SkipRecord

        def modify_record(self, record):
            record['value'] = record['value'].replace('bad words', '@!#$*%&')


    class OneIngestor(Ingestor):
        filename_pattern = r'data\.\d{8}_\d{4}\.csv(\.gz)?'
        tables = [
            {
                'table': 'my_first_table',
                'csv_columns': ('their_id', 'their_value'),
                'column_map': {'their_id': 'id', 'their_value': 'value'},
                'on_conflict': '(id) DO UPDATE SET value = excluded.value',
            }
        ]

    class AnotherIngestor(Ingestor):
        filename_pattern = r'other_data\.\d{8}\.csv(\.gz)?'
        csv_picker = MyPicker
        tables = [
            {
                'table': 'my_other_table',
                'csv_columns': ('id', 'value'),
                'on_conflict': '(id) DO UPDATE SET value = excluded.value',
            },
            {
                'table': 'a_third_table',
                'csv_columns': ('id', 'metadata'),
                'on_conflict': '(id) DO NOTHING',
            }
        ]

    ingest_file('data.20240910_1430.csv.gz')
    ingest_file('other_data.20240910.csv')

Each `Ingestor` subclass will be tried in turn until one matches the filename, and that one will
be used to parse and load the data into its DB tables.