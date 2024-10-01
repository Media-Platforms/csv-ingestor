import csv
import gzip
import io
import logging
import re
from datetime import datetime, timedelta
from os import environ
from os.path import expandvars

import sqlalchemy as sa


class CSVPicker(io.StringIO):
    """File-like object that reads only some columns of a CSV file."""
    def __init__(self, file, columns):
        super().__init__()
        self.reader = csv.DictReader(file)
        self.writer = csv.DictWriter(self, columns, extrasaction='ignore')
        self.total_records = 0

    def read(self, size=-1):
        while True:
            try:
                record = next(self.reader)
            except StopIteration:
                return b''
            try:
                self.modify_record(record)
                self.check_skip(record)
            except SkipRecord:
                continue
            self.truncate(0)
            self.seek(0)
            self.writer.writerow(record)
            self.total_records += 1
            return self.getvalue()

    def check_skip(self, record: dict):
        pass

    def modify_record(self, record: dict):
        pass


class Ingestor:
    """Base for classes that can copy data from CSV files into database tables."""
    csv_picker = CSVPicker
    filename_pattern: str | None = None
    date_format = '%Y%m%d'
    tables: list[dict] = []
    match = None
    conn_str = expandvars('postgresql://$PGUSER:$PGPASSWORD@{PGHOST}:$PGPORT/$PGDATABASE')

    def __init_subclass__(cls):
        INGESTORS.append(cls)

    def __init__(self, filepath):
        self.filepath = filepath
        self.opener = gzip.open if filepath.endswith('.gz') else open
        self.db = sa.create_engine(self.conn_str.format(**environ))

    def matches(self):
        self.match = re.search(self.filename_pattern, self.filepath)
        return self.match

    def ingest(self):
        for table in self.tables:
            try:
                self.ingest_to_table(table)
            except Exception as e:  # pragma: no cover
                logging.error(e)

    def ingest_to_table(self, table):
        table_name = table['table']
        on_conflict = table['on_conflict']
        csv_columns = table['csv_columns']
        column_map = table.get('column_map', {})
        db_columns = ', '.join(column_map.get(c, self.convert(c)) for c in table['csv_columns'])

        # Create partitions for all the records' dates if they don't already exist
        if 'partitioned_on' in table and 'date' in self.match.groupdict():
            self.create_partitions(table)

        # Load the data from the file into the table or partition
        with self.db.begin() as conn, self.opener(self.filepath, 'rt', newline='') as file_stream:
            data_stream = self.csv_picker(file_stream, csv_columns)
            result = self.copy_records(data_stream, conn, table_name, db_columns, on_conflict)
            logging.warning('COPY %s FROM %s TOTAL %s RESULT %s',
                            table_name, self.filepath, data_stream.total_records, result)

    def create_partitions(self, table):
        date = datetime.strptime(self.match['date'], self.date_format)
        self.create_partition(table['table'], date, table['partitioned_on'],
                              table['partition_range'])

    def convert(self, column_name):
        return column_name

    def copy_records(self, stream, conn, table, db_columns, on_conflict=None):
        if on_conflict:
            return self.bulk_insert(stream, conn, table, db_columns, on_conflict)
        do_copy = conn.connection.cursor().copy_expert
        do_copy(SQL.copy_from.format(table, db_columns), stream)

    def bulk_insert(self, stream, conn, table, db_columns, on_conflict=None):
        # This implements a bulk insert that skips existing records, by copying into a temp
        # table, then inserting those records into the real table while ignoring conflicts.
        temp_table = table + '_ingest'
        self.do_sql(SQL.create_temp_table.format(temp_table, table), conn=conn)
        self.copy_records(stream, conn, temp_table, db_columns)
        self.do_sql(SQL.insert_from.format(table, temp_table, on_conflict), conn=conn)

    def create_partition(self, table, date, partitioned_on, partition_days):
        """Create and index a new partition for this file's data if needed."""

        partition = partitioned_on + date.strftime('_%Y_%m_%d')
        date_from = date.strftime('%Y-%m-%d')
        date_to = (date + timedelta(days=partition_days)).strftime('%Y-%m-%d')

        self.do_sql('SET lock_timeout TO 500')  # So we don't block forever on partition add/drop
        self.do_sql(SQL.create_schema.format(table))
        self.do_sql(SQL.create_partition.format(table, partition), (date_from, date_to))
        self.do_sql(SQL.create_index.format(table, partition, partitioned_on))

    def do_sql(self, sql, params=None, conn=None):
        if conn is None:
            conn = self.db
        return conn.execute(sa.text(sql), params)


class SQL:

    copy_from = 'COPY {0} ({1}) FROM STDIN (FORMAT csv)'
    create_index = 'CREATE INDEX IF NOT EXISTS {1}_idx ON {0}.{1}({2})'
    create_table = 'CREATE TABLE IF NOT EXISTS {0}.{1}'
    create_partition = create_table + ' PARTITION OF {0} FOR VALUES FROM (%s) TO (%s)'
    create_schema = 'CREATE SCHEMA IF NOT EXISTS {0}'
    create_temp_table = 'CREATE TEMP TABLE {0} (LIKE {1} INCLUDING DEFAULTS) ON COMMIT DROP'
    drop_table = 'DROP TABLE IF EXISTS {0}.{1}'
    insert_from = 'INSERT INTO {0} SELECT * FROM {1} ON CONFLICT {2}'
    list_partitions = ("SELECT table_name FROM information_schema.tables "
                       "WHERE table_schema='{0}' AND table_type='BASE TABLE'")


INGESTORS: list[type[Ingestor]] = []


def ingest_file(filepath: str):
    """Check the ingestors until one says it likes the filepath, then call it to ingest the file."""
    for cls in INGESTORS:
        ingestor = cls(filepath)
        if ingestor.matches():
            ingestor.ingest()
            break
    else:
        raise NoIngestorFound('No ingestor for filename: ' + filepath)


class SkipRecord(Exception):
    pass


class NoIngestorFound(Exception):
    pass
