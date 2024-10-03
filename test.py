import io
import os
from datetime import date, datetime
from unittest import TestCase
from unittest.mock import ANY, MagicMock, patch

from csv_ingestor import CSVPicker, Ingestor, NoIngestorFound, SkipRecord, ingest_file


class MockPicker(CSVPicker):
    def check_skip(self, record):
        if record['a'] == 'SKIP':
            raise SkipRecord


class MockIngestor(Ingestor):
    filename_pattern = r'test_data.(?P<date>\d{8}).csv.gz'
    csv_picker = MockPicker


class MockLogger:
    warning = error = lambda *args: None


@patch('csv_ingestor.logging', new=MockLogger)
@patch('sqlalchemy.create_engine')
@patch.dict(os.environ, {'PGHOST': 'db.example.com'})
class IngestorTests(TestCase):

    def test_base_picker(self, mock_create_engine):
        stream = io.StringIO('a,b,c\n1,2,3\n')
        columns = 'a', 'c'
        c = CSVPicker(stream, columns)
        assert c.read() == '1,3\r\n'
        assert c.read() == b''


    def test_skip_picker(self, mock_create_engine):
        stream = io.StringIO('a,b,c\nSKIP,2,3\n1,2,3\n')
        columns = 'a', 'c'
        c = MockPicker(stream, columns)
        assert c.read() == '1,3\r\n'
        assert c.read() == b''


    @patch('csv_ingestor.Ingestor.ingest')
    def test_ingest_file(self, mock_create_engine, mock_ingest):
        ingest_file('/some/path/to/test_data.20240512.csv.gz')
        assert mock_ingest.called


    def test_ingest_file_no_match(self, mock_create_engine):
        with self.assertRaises(NoIngestorFound):
            ingest_file('/some/path/to/test_bad.20240512.csv.gz')


    @patch('gzip.open', return_value=io.StringIO())
    def test_Ingestor_on_conflict_do_nothing(self, mock_create_engine, mock_gzip):
        t = {
            'table': 'foo',
            'csv_columns': ('bar', 'baz'),
            'on_conflict': 'DO NOTHING',
        }
        ing = MockIngestor('/foo/bar.gz')
        ing.copy_records = MagicMock()
        ing.ingest_to_table(t)
        ing.copy_records.assert_called_with(ANY, ANY, 'foo', 'bar, baz', 'DO NOTHING')


    def test_Ingestor_copy_new_records(self, mock_create_engine):
        conn = MagicMock()
        stream = MagicMock()
        db_columns = 'foo', 'bar', 'baz'

        ing = MockIngestor('/foo/bar.gz')
        ing.bulk_insert(stream, conn, 'some_table', db_columns, 'DO NOTHING')

        assert str(conn.execute.mock_calls[0][1][0]) == (
            'CREATE TEMP TABLE some_table_ingest (LIKE some_table INCLUDING DEFAULTS) '
            'ON COMMIT DROP'
        )
        assert conn.connection.mock_calls[1][1][0] == (
            "COPY some_table_ingest (('foo', 'bar', 'baz')) FROM STDIN (FORMAT csv)"
        )
        assert str(conn.execute.mock_calls[1][1][0]) == (
            'INSERT INTO some_table SELECT * FROM some_table_ingest ON CONFLICT DO NOTHING'
        )


    @patch('csv_ingestor.Ingestor.ingest_to_table', side_effect=KeyError)
    def test_ingest_setup_cleanup(self, mock_ingest_to_table, mock_create_engine):
        ing = MockIngestor('/some/path/to/test_data.20240512.csv.gz')
        ing.tables = [
            {
                'table': 'foo',
                'csv_columns': ('bar', 'baz'),
                'on_conflict': 'DO NOTHING',
            },
        ]
        ing.setup_sql = 'SELECT 1'
        ing.cleanup_sql = 'SELECT 2'
        ing.ingest()
        assert mock_ingest_to_table.called
        assert str(mock_create_engine.mock_calls[1][1][0]) == 'SELECT 1'
        assert str(mock_create_engine.mock_calls[2][1][0]) == 'SELECT 2'


    def test_Ingestor_create_partition(self, mock_create_engine):
        ing = Ingestor('/some/file.csv')
        ing.create_partition('temp_table', date(2019, 6, 12), 'partitioned_on_col', 1)
        assert 'temp_table.partitioned_on_col' in str(mock_create_engine.mock_calls[3][1][0])


    @patch('gzip.open', side_effect=[io.StringIO(), io.StringIO()])
    def test_ingest_file_with_partitioned_column(self, mock_create_engine, mock_gzip):
        mock_begin = mock_create_engine.return_value.begin
        mock_begin.return_value.__enter__.return_value.execute = MagicMock(side_effect=[
            MagicMock(fetchall=MagicMock(return_value=[])),
            MagicMock(fetchone=MagicMock(return_value={'post_created_at': datetime.now()}))
        ])
        table = {
            'table': 'sp_stats',
            'csv_columns': ('url', 'stat_name'),
            'column_map': {'url': 'posts_url'},
            'partitioned_on': 'some_col',
            'partition_range': 1,
            'on_conflict': 'DO NOTHING',
        }

        ing = MockIngestor('test_data.20240512.csv.gz')
        cp = ing.create_partition = MagicMock()
        cp.side_effect = ['other_partition_name', 'other_other_partition_name']
        ing.copy_records = MagicMock()
        ing.csv_picker = MagicMock()
        ing.csv_picker.return_value.read.side_effect = ['some_url,', b'']

        if ing.matches():
            ing.ingest_to_table(table)

        ing.copy_records.assert_called_with(ANY, ANY, 'sp_stats', 'posts_url, stat_name', 'DO NOTHING')
        cp.assert_called_with(table['table'], ANY, table['partitioned_on'], 1)


    @patch('gzip.open', side_effect=[io.StringIO(), io.StringIO()])
    def test_ingest_file_with_partitioned_column_bad(self, mock_create_engine, mock_gzip):
        mock_begin = mock_create_engine.return_value.begin
        mock_begin.return_value.__enter__.return_value.execute = MagicMock(side_effect=[
            MagicMock(fetchall=MagicMock(return_value=[])),
            MagicMock(fetchone=MagicMock(return_value={}))
        ])
        table = {
            'table': 'sp_stats',
            'csv_columns': ('url', 'stat_name'),
            'column_map': {'url': 'posts_url'},
            'partitioned_on': 'some_col',
            'partition_range': 1,
            'on_conflict': 'DO NOTHING',
        }

        ing = MockIngestor('test_data.20240512.csv.gz')
        cp = ing.create_partition = MagicMock()
        cp.side_effect = ['other_partition_name', 'other_other_partition_name']
        ing.copy_records = MagicMock()
        ing.csv_picker = MagicMock()
        ing.csv_picker.return_value.read.side_effect = ['some_url,', b'']
        if ing.matches():
            ing.ingest_to_table(table)

        ing.copy_records.assert_called_with(ANY, ANY, 'sp_stats', 'posts_url, stat_name', 'DO NOTHING')
        cp.assert_called_with(table['table'], ANY, table['partitioned_on'], 1)
