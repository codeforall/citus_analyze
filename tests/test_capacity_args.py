from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from capacity_args import parse_size_mib, resolve_capacities


class CapacityArgsTests(unittest.TestCase):
    def test_units_and_bare_mib(self):
        for source, expected in {
            '32GB': '32768', '64MB': '64', '1TB': '1048576', '32GiB': '32768',
            '1.5gb': '1536', ' 64 MiB ': '64', '32000': '32000', '00064': '64',
            '512KB': '0.5', '0.5KiB': '0.00048828125', '1B': '0.00000095367431640625',
            '0GB': '0', '0': '0', '1.5': '1.5', '0.0009765625GB': '1'
        }.items():
            with self.subTest(source=source):
                self.assertEqual(parse_size_mib(source), expected)

    def test_invalid_sizes_are_rejected(self):
        for value in ('', ' ', '-1', '-32GB', '+32GB', 'NaN', 'Inf', '1e3MB', 'GB',
                      '32GBextra', '32PB', '32G', '1.2.3GB', '1;SELECT 1', '1|2',
                      '0.1B', '0.1MB', '9223372036854775808B', '9' * 65):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    parse_size_mib(value)

    def test_maximum_and_small_sizes_remain_exact(self):
        self.assertEqual(parse_size_mib('9223372036854775807B'), '8796093022207.99999904632568359375')
        self.assertNotEqual(parse_size_mib('1KB'), '0')

    def test_coordinator_fallback_is_per_resource(self):
        values, sources = resolve_capacities(coord_ram='32GB', coord_disk='1TB')
        self.assertEqual(values, {'coord_ram': '32768', 'worker_ram': '32768',
                                  'coord_disk': '1048576', 'worker_disk': '1048576'})
        self.assertEqual(sources['worker_ram'], 'assumed same as coordinator')
        values, _ = resolve_capacities(coord_ram='32GB', worker_disk='256GB')
        self.assertEqual(values, {'coord_ram': '32768', 'worker_ram': '32768',
                                  'coord_disk': '', 'worker_disk': '262144'})

    def test_role_overrides_shared_defaults(self):
        values, sources = resolve_capacities(node_ram='32GB', node_disk='1TB',
                                             coord_ram='16GB', worker_disk='2TB')
        self.assertEqual(values, {'coord_ram': '16384', 'worker_ram': '32768',
                                  'coord_disk': '1048576', 'worker_disk': '2097152'})
        self.assertEqual(sources['worker_ram'], 'shared node value')
        self.assertEqual(sources['worker_disk'], 'explicit worker value')

    def test_zero_is_explicit_and_missing_stays_missing(self):
        values, sources = resolve_capacities(coord_ram='32GB', worker_ram='0', node_disk='0')
        self.assertEqual(values['worker_ram'], '0')
        self.assertEqual(sources['worker_ram'], 'explicit worker value')
        self.assertEqual(values['worker_disk'], '0')
        values, _ = resolve_capacities(worker_ram='16GB')
        self.assertEqual(values['coord_ram'], '')
        self.assertEqual(values['worker_ram'], '16384')
        values, _ = resolve_capacities()
        self.assertTrue(all(value == '' for value in values.values()))

    def test_invalid_shared_value_is_not_ignored_when_overridden(self):
        with self.assertRaisesRegex(ValueError, '--node-ram:'):
            resolve_capacities(node_ram='wrong', coord_ram='32GB', worker_ram='32GB')


if __name__ == '__main__':
    unittest.main()