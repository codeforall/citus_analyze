from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / 'bin/citus_analyze'


class CliTests(unittest.TestCase):
    def test_syntax(self):
        subprocess.run(['bash', '-n', str(DRIVER)], check=True)

    def test_options_before_connection(self):
        for options in (['--coord-ram-mb=32000', '--help'], ['--coord-ram-mb', '32000', '--help'], ['--advisor-var=m1_peak_active=20', '--help']):
            self.assertEqual(subprocess.run(['bash', str(DRIVER), *options], capture_output=True).returncode, 0)
        for options in (['--coord-ram-mb'], ['--fail-on=nonsense'], ['--output-format=nonsense'], ['--coord-ram-mb=-1'], ['--advisor-var=ON_ERROR_STOP=0'], ['--advisor-var=bad=SQL']):
            self.assertEqual(subprocess.run(['bash', str(DRIVER), *options], capture_output=True).returncode, 2)

    def test_failed_advisors_fail_even_with_none_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / 'psql'
            fake.write_text('#!/bin/bash\ncase "$*" in *citus_version*) printf "version\\n13.3\\n";; *) echo "ERROR: test collection failure" >&2; exit 3;; esac\n')
            fake.chmod(0o700)
            result = subprocess.run(['bash', str(DRIVER), '--psql', str(fake), '--no-password',
                                     '--advisors-only', '--fail-on=none', '--out-dir', str(root / 'report')],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn('OVERALL: INCOMPLETE', result.stdout)
            self.assertTrue((root / 'report/M1.json').exists())

    def test_info_exit_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / 'psql'
            fake.write_text('#!/bin/bash\ncase "$*" in *citus_version*) printf "version\\n13.3\\n";; *) echo "INFO : scenario only";; esac\n')
            fake.chmod(0o700)
            result = subprocess.run(['bash', str(DRIVER), '--psql', str(fake), '--no-password',
                                     '--advisors-only', '--fail-on=info', '--out-dir', str(root / 'report')],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertNotIn('COLLECTION: INCOMPLETE', result.stdout)


if __name__ == '__main__':
    unittest.main()