import json
import unittest

import test_sql


@unittest.skipUnless(test_sql.BINDIR, 'Set CITUS_TEST_BINDIR for memory SQL tests')
class MemoryCapacityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_sql.SqlTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        test_sql.SqlTests.tearDownClass()

    def plan(self, *variables):
        output = test_sql.SqlTests.sql(file=test_sql.ROOT / 'sql/advisors/m1_node_memory_minimum.sql', variables=variables)
        return next(json.loads(line.split('=', 1)[1]) for line in output.splitlines() if line.startswith('M1_CAPACITY_JSON='))

    def test_limit_fits_and_next_connection_does_not(self):
        capacity = (test_sql.ROOT / 'sql/memory_capacity.sql').read_text()
        capacity = capacity.replace('DROP TABLE pg_temp._m1_capacity;', """
            DO $$ DECLARE row record; used numeric; next_used numeric; BEGIN
              FOR row IN SELECT * FROM _m1_capacity WHERE inputs_valid LOOP
                used := row.fixed_mib + row.connection_limit * row.per_connection_mib
                  + (ceil(row.connection_limit * row.active_fraction)
                    + least(ceil(row.connection_limit * row.active_fraction) * (row.settings->>'per_gather')::numeric,
                            (row.settings->>'parallel')::numeric)) * row.operator_mib_per_process;
                next_used := row.fixed_mib + (row.connection_limit+1) * row.per_connection_mib
                  + (ceil((row.connection_limit+1) * row.active_fraction)
                    + least(ceil((row.connection_limit+1) * row.active_fraction) * (row.settings->>'per_gather')::numeric,
                            (row.settings->>'parallel')::numeric)) * row.operator_mib_per_process;
                IF row.connection_limit > 0 AND used > row.budget_mib THEN
                  RAISE EXCEPTION 'limit exceeds memory budget'; END IF;
                IF row.connection_limit < row.connection_slots AND next_used <= row.budget_mib THEN
                  RAISE EXCEPTION 'limit unnecessarily low'; END IF;
                IF row.extra_shards > 0 AND row.growth_baseline_mib + row.extra_shards*row.per_new_shard_mib > row.budget_mib THEN
                  RAISE EXCEPTION 'shard growth exceeds budget'; END IF;
                IF row.extra_tables > 0 AND row.growth_baseline_mib + row.extra_tables*row.per_new_table_mib > row.budget_mib THEN
                  RAISE EXCEPTION 'table growth exceeds budget'; END IF;
              END LOOP;
            END $$;
            DROP TABLE pg_temp._m1_capacity;
        """)
        source = (test_sql.ROOT / 'sql/advisors/m1_node_memory_minimum.sql').read_text().replace('\\ir ../memory_capacity.sql', capacity)
        test_sql.SqlTests.sql(source, variables=('coord_ram_mb=2000', 'worker_ram_mb=2000', 'm1_peak_connected=10',
                                                'm1_peak_active=2', 'm1_growth_cache_pct=25', 'm1_capacity_active_pct=50'))

    def test_failed_nodes_hide_capacity_numbers(self):
      source = (test_sql.ROOT / 'sql/advisors/m1_node_memory_minimum.sql').read_text()
      source = source.replace('CREATE TEMP TABLE _m1_calc AS', """
        UPDATE _m1_raw SET success=false, result='unavailable' WHERE nodeid=(SELECT max(nodeid) FROM _m1_raw);
        CREATE TEMP TABLE _m1_calc AS
      """)
      output = test_sql.SqlTests.sql(source, variables=('coord_ram_mb=2000', 'worker_ram_mb=2000'))
      result = next(json.loads(line.split('=', 1)[1]) for line in output.splitlines() if line.startswith('M1_CAPACITY_JSON='))
      self.assertFalse(result['all_nodes_measured'])
      self.assertIsNone(result['cluster_extra_shards'])
      self.assertTrue(all(node['connection_limit'] is None and node['extra_connections'] is None for node in result['nodes']))

    def test_invalid_inputs_and_no_headroom(self):
      for override in ('m1_capacity_active_pct=0', 'm1_capacity_active_pct=101', 'm1_capacity_headroom_pct=100', 'm1_growth_cache_pct=101'):
        result = self.plan('coord_ram_mb=2000', 'worker_ram_mb=2000', override)
        self.assertTrue(all(node['connection_limit'] is None for node in result['nodes']))
      result = self.plan('coord_ram_mb=100', 'worker_ram_mb=100', 'm1_peak_connected=10', 'm1_peak_active=2', 'm1_growth_cache_pct=25')
      self.assertTrue(all(node['connection_limit'] == 0 and node['extra_connections'] == 0 for node in result['nodes']))
      self.assertEqual(result['cluster_extra_shards'], 0)
      self.assertEqual(result['cluster_extra_tables'], 0)

    def test_larger_new_shards_reduce_growth(self):
      common = ('coord_ram_mb=2000', 'worker_ram_mb=2000', 'm1_peak_connected=10', 'm1_peak_active=2', 'm1_growth_cache_pct=25')
      small = self.plan(*common, 'm1_growth_shard_mb=128')
      large = self.plan(*common, 'm1_growth_shard_mb=1024')
      self.assertGreater(small['cluster_extra_shards'], large['cluster_extra_shards'])
      self.assertGreaterEqual(small['cluster_extra_tables'], large['cluster_extra_tables'])

    def test_missing_inputs_and_workload_effect(self):
        missing = self.plan()
        self.assertEqual(len(missing['nodes']), 3)
        self.assertTrue(all(node['connection_limit'] is None for node in missing['nodes']))
        self.assertIsNone(missing['cluster_extra_shards'])
        common = ('coord_ram_mb=2000', 'worker_ram_mb=2000', 'm1_peak_connected=10', 'm1_peak_active=2', 'm1_growth_cache_pct=25')
        busy = self.plan(*common, 'm1_capacity_active_pct=100')
        mixed = self.plan(*common, 'm1_capacity_active_pct=20')
        higher = self.plan(*common, 'm1_capacity_headroom_pct=40')
        for full, partial, reserved in zip(busy['nodes'], mixed['nodes'], higher['nodes']):
            self.assertGreaterEqual(partial['connection_limit'], full['connection_limit'])
            self.assertLessEqual(reserved['connection_limit'], full['connection_limit'])
        self.assertIsNotNone(busy['cluster_extra_shards'])
        self.assertEqual(busy['cluster_extra_shards'], min(node['extra_shards'] for node in busy['nodes']))
        self.assertIsNone(self.plan('coord_ram_mb=32000', 'worker_ram_mb=32000')['cluster_extra_tables'])


if __name__ == '__main__':
    unittest.main()