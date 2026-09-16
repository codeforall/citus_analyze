import json
import unittest

import test_sql


@unittest.skipUnless(test_sql.BINDIR, 'Set CITUS_TEST_BINDIR for application-capacity SQL tests')
class ApplicationCapacityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_sql.SqlTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        test_sql.SqlTests.tearDownClass()

    def plan(self, setting, *variables):
        source = (test_sql.ROOT / 'sql/advisors/m1_node_memory_minimum.sql').read_text()
        source = source.replace('CREATE TEMP TABLE _m1_calc AS', f"""
            UPDATE _m1_raw SET result=(result::jsonb || jsonb_build_object('max_client_connections', {setting}))::text WHERE success;
            CREATE TEMP TABLE _m1_calc AS
        """)
        output = test_sql.SqlTests.sql(source, variables=('coord_ram_mb=32000', 'worker_ram_mb=32000', *variables))
        return next(json.loads(line.split('=', 1)[1]) for line in output.splitlines() if line.startswith('M1_CAPACITY_JSON='))

    def test_positive_cap_is_separate_from_total_database_limit(self):
        result = self.plan("'12'", 'm1_internal_connections=10')
        for node in result['nodes']:
            self.assertGreater(node['connection_limit'], 12)
            self.assertEqual(node['application_connection_limit'], 12)
            self.assertEqual(node['citus_client_limit'], 12)

    def test_zero_and_disabled(self):
        for node in self.plan("'0'", 'm1_internal_connections=10')['nodes']:
            self.assertEqual(node['application_connection_limit'], 0)
            self.assertGreater(node['connection_limit'], 0)
            self.assertEqual(node['citus_client_limit_status'], 'blocked')
        for node in self.plan("'-1'", 'm1_internal_connections=10', 'm1_other_client_connections=3')['nodes']:
            self.assertEqual(node['application_connection_limit'], node['connection_limit'] - 13)
            self.assertIsNone(node['citus_client_limit'])
            self.assertEqual(node['citus_client_limit_status'], 'disabled')

    def test_other_clients_reduce_both_limits(self):
        for node in self.plan("'12'", 'm1_internal_connections=10', 'm1_other_client_connections=3')['nodes']:
            self.assertEqual(node['application_connection_limit'], 9)
        for node in self.plan("'12'", 'm1_internal_connections=1000')['nodes']:
            self.assertEqual(node['application_connection_limit'], 0)

    def test_unknown_setting_or_missing_allowance_preserves_total_only(self):
        for setting in ('NULL', "'auto'", "'-2'"):
            for node in self.plan(setting, 'm1_internal_connections=10')['nodes']:
                self.assertIsNotNone(node['connection_limit'])
                self.assertIsNone(node['application_connection_limit'])
                self.assertEqual(node['citus_client_limit_status'], 'unknown')
        for node in self.plan("'12'")['nodes']:
            self.assertIsNone(node['application_connection_limit'])
            self.assertIsNotNone(node['connection_limit'])
        for node in self.plan("'12'", 'm1_internal_connections=1.5')['nodes']:
            self.assertIsNone(node['application_connection_limit'])

    def test_heterogeneous_client_caps(self):
        result = self.plan("CASE WHEN nodeid=(SELECT min(nodeid) FROM _m1_raw) THEN '2' ELSE '4' END", 'm1_internal_connections=5')
        self.assertEqual([node['application_connection_limit'] for node in result['nodes']], [2, 4, 4])


if __name__ == '__main__':
    unittest.main()