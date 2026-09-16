from pathlib import Path
import subprocess
import unittest

import test_sql


@unittest.skipUnless(test_sql.BINDIR, 'Set CITUS_TEST_BINDIR for local client-admission tests')
class ClientAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_sql.SqlTests.setUpClass()
        test_sql.SqlTests.sql('CREATE ROLE capacity_test_client LOGIN;')

    @classmethod
    def tearDownClass(cls):
        test_sql.SqlTests.tearDownClass()

    def restart_coordinator(self, setting):
        fixture = test_sql.SqlTests
        data, port = fixture.nodes[0]
        options = (f'-p {port} -h 127.0.0.1 -k {fixture.root} '
                   '-c shared_preload_libraries=citus -c max_prepared_transactions=100 '
                   '-c shared_buffers=16MB -c citus.enable_statistics_collection=off '
                   f'-c citus.max_client_connections={setting}')
        fixture.command('pg_ctl', '-D', str(data), '-l', str(fixture.root / 'node0.log'),
                        '-o', options, '-m', 'fast', '-w', 'restart')

    def connect_regular_client(self):
        return subprocess.run([str(Path(test_sql.BINDIR) / 'psql'), '-X', '-w', '-h', '127.0.0.1',
                               '-p', str(test_sql.SqlTests.nodes[0][1]), '-U', 'capacity_test_client',
                               '-d', 'postgres', '-Atc', 'SELECT 1'], capture_output=True, text=True,
                              env=test_sql.SqlTests.env, timeout=30)

    def test_zero_blocks_regular_clients_and_minus_one_disables_cap(self):
        try:
            self.restart_coordinator(0)
            result = self.connect_regular_client()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('configured to accept up to 0 regular client connections', result.stderr)
            self.assertIn('1', test_sql.SqlTests.sql('SELECT 1;'))
            self.restart_coordinator(-1)
            result = self.connect_regular_client()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), '1')
        finally:
            self.restart_coordinator(-1)