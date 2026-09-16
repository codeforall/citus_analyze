from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / 'bin/citus_analyze'


class CapacityCliTests(unittest.TestCase):
    def run_driver(self, arguments):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / 'psql'
            fake.write_text('#!/bin/bash\ncase "$*" in *citus_version*) printf "version\\n15.0\\n";; *) printf "INFO : fixture\\n"; printf "%s\\n" "$@";; esac\n')
            fake.chmod(0o700)
            result = subprocess.run(['bash', str(DRIVER), '--psql', str(fake), '--no-password',
                                     '--advisors-only', '--fail-on=none', '--out-dir', str(root / 'report'), *arguments],
                                    capture_output=True, text=True)
            output = root / 'report/M1.out'
            lines = output.read_text().splitlines() if output.exists() else []
            variables = dict(lines[index + 1].split('=', 1) for index, line in enumerate(lines[:-1]) if line == '-v')
            return result, variables

    def assert_capacities(self, arguments, expected):
        result, variables = self.run_driver(arguments)
        self.assertEqual(result.returncode, 0, result.stderr)
        capacity_keys = ('coord_ram_mb', 'worker_ram_mb', 'coord_disk_mb', 'worker_disk_mb')
        self.assertEqual({key: variables[key] for key in capacity_keys if key in variables}, expected)
        return result

    def test_coordinator_values_fill_workers(self):
        result = self.assert_capacities(['--coord-ram', '32GB', '--coord-disk=1TB'],
                                       dict(coord_ram_mb='32768', worker_ram_mb='32768',
                                            coord_disk_mb='1048576', worker_disk_mb='1048576'))
        self.assertEqual(result.stdout.count('assumed same as coordinator'), 2)

    def test_shared_specs_and_fractional_units(self):
        self.assert_capacities(['--node-ram=1.5GiB', '--node-disk-size', '2tb'],
                               dict(coord_ram_mb='1536', worker_ram_mb='1536',
                                    coord_disk_mb='2097152', worker_disk_mb='2097152'))

    def test_specific_values_override_shared_independently_of_order(self):
        expected = dict(coord_ram_mb='16384', worker_ram_mb='65536', coord_disk_mb='1048576', worker_disk_mb='2097152')
        shared = ['--node-ram=32GB', '--node-disk=1TB']
        specific = ['--coord-ram=16GB', '--worker-ram=64GB', '--worker-disk-size=2TB']
        self.assert_capacities(shared + specific, expected)
        self.assert_capacities(specific + shared, expected)

    def test_worker_only_and_legacy_zero(self):
        self.assert_capacities(['--worker-ram=64MB', '--worker-disk=512KiB'],
                               dict(worker_ram_mb='64', worker_disk_mb='0.5'))
        self.assert_capacities(['--coord-ram-mb=32000', '--worker-ram-mb=0',
                                '--coord-disk-mb=102400', '--worker-disk-mb=0'],
                               dict(coord_ram_mb='32000', worker_ram_mb='0', coord_disk_mb='102400', worker_disk_mb='0'))

    def test_last_alias_wins_for_the_same_role(self):
        self.assert_capacities(['--coord-ram-mb=100', '--coord-ram=64MB', '--coord-disk-size=1GB'],
                               dict(coord_ram_mb='64', worker_ram_mb='64', coord_disk_mb='1024', worker_disk_mb='1024'))

    def test_invalid_values_fail_before_psql(self):
        for arguments in (['--coord-ram'], ['--node-disk-size'], ['--node-ram=-1GB'],
                          ['--worker-ram=invalid'], ['--coord-disk=1PB'], ['--node-ram=1e6MB'],
                          ['--node-disk-size=1TBextra'], ['--coord-ram=0.1B']):
            with self.subTest(arguments=arguments):
                result, variables = self.run_driver(arguments)
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertEqual(variables, {})
                self.assertNotIn('connected OK', result.stdout)

    def test_help_documents_units_and_defaults(self):
        result = subprocess.run(['bash', str(DRIVER), '--help'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        for phrase in ('--coord-ram=SIZE', '--node-disk-size=SIZE', '1024-based', 'workers inherit', 'Legacy'):
            self.assertIn(phrase, result.stdout)


if __name__ == '__main__':
    unittest.main()