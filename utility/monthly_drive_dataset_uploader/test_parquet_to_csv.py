import gzip
import tempfile
import unittest
import zipfile
from datetime import datetime
from pathlib import Path

import polars as pl
from parquet_to_csv import convert


class ConverterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def source(self, name='aws-2025-03-15.parquet', **values):
        path = self.root / name
        frame = pl.DataFrame(values or {
            'Time': [datetime(2025, 3, 15, 0, 10), datetime(2025, 3, 15), datetime(2025, 3, 15, 0, 10)],
            'SPS': [3, None, 5], 'IF': [1.5, -1., None]})
        frame.write_parquet(path)
        return path

    def test_tick_batches_keep_all_rows_and_nulls(self):
        src = self.source()
        out = self.root / 'ticks'
        self.assertEqual(convert(src, out, mode='tick', batch_size=1), {'files': 2, 'rows': 3})
        tick = out / 'aws-2025-03/15/00-10-00.csv.gz'
        self.assertEqual(pl.read_csv(tick)['SPS'].to_list(), [3, 5])
        self.assertEqual(pl.read_csv(out / 'aws-2025-03/15/00-00-00.csv.gz')['SPS'].to_list(), [None])
        with gzip.open(tick, 'rt') as f:
            self.assertEqual(f.read().count('Time,SPS,IF'), 1)
        self.assertNotIn('id', pl.read_csv(tick).columns)
        self.assertNotIn('SPS_Update_Time', pl.read_csv(tick).columns)

    def test_combined_historical_types_and_missing_columns(self):
        a = self.source('azure-2025-12-01.parquet', Time=[datetime(2025, 12, 1)], Score=['Low'], T2=[None])
        b = self.source('azure-2025-12-12.parquet', Time=[datetime(2025, 12, 12)], Score=[3], T2=[2.], DesiredCount=[1])
        out = self.root / 'combined.csv.gz'
        convert([a, b], out, sort_by_time=True)
        df = pl.read_csv(out)
        self.assertEqual(df['Score'].to_list(), ['Low', '3'])
        self.assertEqual(df['T2'].to_list(), [None, 2.])
        self.assertEqual(df['DesiredCount'].to_list(), [None, 1])

    def test_zip_glob_and_duplicate_input(self):
        src = self.source()
        archive = self.root / 'aws-2025-03.zip'
        with zipfile.ZipFile(archive, 'w') as z:
            z.write(src, f'aws-2025-03/{src.name}')
        out = self.root / 'out.csv'
        convert([str(self.root / '*.zip'), archive], out)
        self.assertEqual(pl.read_csv(out).height, 3)

    def test_zip_traversal_rejected(self):
        src = self.source()
        archive = self.root / 'unsafe.zip'
        with zipfile.ZipFile(archive, 'w') as z:
            z.write(src, '../outside.parquet')
        with self.assertRaisesRegex(ValueError, 'Unsafe ZIP path'):
            convert(archive, self.root / 'out.csv')
        self.assertFalse((self.root / 'out.csv').exists())

    def test_overlapping_snapshots_leave_no_output(self):
        first = self.source()
        second = self.source('aws-2025-03-16.parquet')
        with self.assertRaisesRegex(ValueError, 'Overlapping'):
            convert([first, second], self.root / 'ticks', mode='tick')
        self.assertFalse((self.root / 'ticks').exists())

    def test_null_and_subsecond_timestamps_rejected(self):
        for value in (None, datetime(2025, 3, 15, microsecond=1)):
            src = self.root / 'aws-2025-03-15.parquet'
            pl.DataFrame({'Time': pl.Series([value], dtype=pl.Datetime('us')), 'SPS': [1]}).write_parquet(src)
            with self.assertRaises(ValueError):
                convert(src, self.root / 'ticks', mode='tick')
            self.assertFalse((self.root / 'ticks').exists())

    def test_provider_and_existing_output_checks(self):
        src = self.source('generic.parquet')
        with self.assertRaisesRegex(ValueError, 'Cannot infer provider'):
            convert(src, self.root / 'ticks', mode='tick')
        convert(src, self.root / 'ticks', mode='tick', provider='aws')
        with self.assertRaises(FileExistsError):
            convert(src, self.root / 'ticks', mode='tick', provider='aws')


if __name__ == '__main__':
    unittest.main()
