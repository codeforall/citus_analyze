import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
BINDIR = os.environ.get('CITUS_TEST_BINDIR')


@unittest.skipUnless(BINDIR, 'Set CITUS_TEST_BINDIR for isolated PostgreSQL/Citus tests')
class SqlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix='citus-analyze-test-')
        cls.root = Path(cls.directory.name)
        cls.nodes = []
        cls.env = {key: value for key, value in os.environ.items() if not key.startswith('PG')}
        try:
            for index in range(3):
                with socket.socket() as probe:
                    probe.bind(('127.0.0.1', 0))
                    port = probe.getsockname()[1]
                data = cls.root / f'node{index}'
                cls.command('initdb', '-D', str(data), '-U', 'postgres', '-A', 'trust', '--no-locale', '-E', 'UTF8')
                options = (f'-p {port} -h 127.0.0.1 -k {cls.root} '
                           '-c shared_preload_libraries=citus -c max_prepared_transactions=100 '
                           '-c shared_buffers=16MB -c citus.enable_statistics_collection=off')
                cls.command('pg_ctl', '-D', str(data), '-l', str(cls.root / f'node{index}.log'), '-o', options, '-w', 'start')
                cls.nodes.append((data, port))
                cls.sql('CREATE EXTENSION citus;', port=port)
            cls.sql(f"SELECT citus_set_coordinator_host('127.0.0.1', {cls.nodes[0][1]});")
            for data, port in cls.nodes[1:]:
                cls.sql(f"SELECT citus_add_node('127.0.0.1', {port});")
            cls.sql("""
                SET citus.shard_count = 4;
                CREATE TABLE accounts (tenant_id int, payload text);
                SELECT create_distributed_table('accounts', 'tenant_id');
                CREATE TABLE empty_history (tenant_id int, payload text);
                SELECT create_distributed_table('empty_history', 'tenant_id', colocate_with => 'accounts');
                INSERT INTO accounts SELECT tenant, repeat('data', 100) FROM generate_series(1, 12000) tenant;
                ANALYZE accounts;
                CREATE TABLE reference_data (id int PRIMARY KEY, label text);
                SELECT create_reference_table('reference_data');
                INSERT INTO reference_data VALUES (1, 'fixture');
                ANALYZE reference_data;
                CREATE TABLE events (tenant_id int, created_at timestamptz) PARTITION BY RANGE(created_at);
                CREATE TABLE events_old PARTITION OF events FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
                CREATE TABLE events_default PARTITION OF events DEFAULT;
                CREATE TABLE local_parent (id int PRIMARY KEY);
                CREATE TABLE local_child (id int REFERENCES local_parent(id));
                CREATE INDEX local_child_id ON local_child(id);
            """)
        except Exception:
            cls.tearDownClass()
            raise

    @classmethod
    def command(cls, executable, *arguments, input=None):
        result = subprocess.run([str(Path(BINDIR) / executable), *arguments], input=input,
                                capture_output=True, text=True, env=cls.env, timeout=120,
                                cwd=ROOT / 'sql/advisors')
        if result.returncode:
            raise AssertionError(f'{executable} failed: {result.stderr[-5000:]}\n{result.stdout[-1000:]}')
        return result

    @classmethod
    def sql(cls, text=None, *, port=None, file=None, variables=()):
        options = ['-X', '-w', '-h', '127.0.0.1', '-p', str(port or cls.nodes[0][1]),
                   '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=on']
        for variable in variables:
            options.extend(['-v', variable])
        if file:
            options.extend(['-f', str(file)])
        result = cls.command('psql', *options, input=text)
        if 'ERROR:' in result.stderr:
            raise AssertionError(result.stderr[-5000:])
        return result.stdout

    @classmethod
    def tearDownClass(cls):
        for data, port in reversed(cls.nodes):
            cls.command('pg_ctl', '-D', str(data), '-m', 'immediate', '-w', 'stop')
        cls.directory.cleanup()

    def test_advisors_execute(self):
        selected = os.environ.get('CITUS_TEST_ADVISORS', '').lower().split(',')
        for file in sorted((ROOT / 'sql/advisors').glob('*.sql')):
            if selected != [''] and file.name.split('_')[0] not in selected:
                continue
            with self.subTest(advisor=file.name):
                output = self.sql(file=file)
                self.assertTrue(any(level in output for level in ('OK', 'INFO', 'WARN', 'CRITICAL', 'INCOMPLETE')), output)

    def test_memory_global_parallel_cap(self):
        source = (ROOT / 'sql/advisors/m1_node_memory_minimum.sql').read_text()
        source = source.replace('DROP TABLE pg_temp._m1_calc;', """
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM _m1_calc WHERE parallel_workers > (settings->>'parallel')::numeric
                  OR parallel_workers > active * (settings->>'per_gather')::numeric
                  OR operator_mib_per_process <> (settings->>'work')::numeric * (1 + (settings->>'hash')::numeric))
              THEN RAISE EXCEPTION 'memory allocation violates global pool/operator model'; END IF;
            END $$;
            DROP TABLE pg_temp._m1_calc;
        """)
        output = self.sql(source,
                          variables=('m1_peak_connected=100', 'm1_peak_active=100', 'coord_ram_mb=32000', 'worker_ram_mb=32000'))
        self.assertIn('scenario_gib', output)
        self.assertNotIn('WILL OOM', output)

    def test_empty_colocated_table_does_not_inflate_skew(self):
        output = self.sql(file=ROOT / 'sql/advisors/s3_data_skew_advisor.sql', variables=('min_group_bytes=0',))
        self.assertNotIn('WARN', output)

    def test_empty_tables_and_unchanged_data_do_not_need_analyze(self):
        source = (ROOT / 'sql/advisors/stat1_stats_freshness.sql').read_text()
        source = source.replace("\\echo '-- STAT1a. Distributed-table stats summary --'", """
            UPDATE _stat1_dist SET total_mods_since_analyze=0,
              oldest_analyze=now()-interval '40 days', newest_analyze=now()-interval '40 days';
            SELECT 'statistics scenario: old but unchanged' AS fixture;
        """)
        output = self.sql(source, variables=('stat1_min_rows=100',))
        self.assertNotIn('CRITICAL', output)
        self.assertNotIn('WARN:', output)

    def test_unchanged_growth_preserves_cache_counts(self):
        source = (ROOT / 'sql/advisors/gr1_shard_growth_advisor.sql').read_text()
        source = source.replace('DROP TABLE pg_temp._gr1;', """
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM _gr1 WHERE t_meta_b <> c_meta_b OR t_placements_total <> placements_now)
              THEN RAISE EXCEPTION 'unchanged target changed placement/cache counts'; END IF;
            END $$;
            DROP TABLE pg_temp._gr1;
        """)
        self.sql(source)

    def test_default_partition_covers_gaps(self):
        output = self.sql(file=ROOT / 'sql/advisors/p1_partition_hygiene.sql')
        self.assertNotIn('future gaps without DEFAULT', output)
        self.assertNotIn('without DEFAULT. Review', output)
        self.assertNotIn('WILL fail', output)

    def test_reference_copies_on_coordinator_are_not_errors(self):
        output = self.sql(file=ROOT / 'sql/advisors/ref1_reference_table_health.sql')
        self.assertNotIn('WARN : reference table(s) have extra placements', output)
        self.assertNotIn('CRITICAL', output)

    def test_fanin_counts_source_sessions(self):
        source = (ROOT / 'sql/advisors/c3_max_external_connections.sql').read_text()
        source = source.replace('DROP TABLE pg_temp._connection_budget;', """
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM _connection_edges WHERE requested <> 32)
              THEN RAISE EXCEPTION 'per-peer demand must include sessions and cached connections'; END IF;
              IF EXISTS (SELECT 1 FROM _connection_budget node WHERE shouldhaveshards AND requested_fanin <>
                32 * (SELECT count(*) FROM _connection_nodes source WHERE source.is_entry AND source.nodeid <> node.nodeid))
              THEN RAISE EXCEPTION 'fanin omits source or non-MX target'; END IF;
            END $$;
            DROP TABLE pg_temp._connection_budget;
        """)
        self.sql(source, variables=('connection_sessions_per_entry=10', 'connections_per_session=3', 'cached_connections_per_peer=2'))

    def test_pool_demand_and_reserve_budget(self):
        source = (ROOT / 'sql/advisors/cp1_pgbouncer_pool.sql').read_text()
        source = source.replace('WITH budgets AS (', 'CREATE TEMP TABLE _pool_test AS WITH budgets AS (')
        source += """
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM _pool_test WHERE all_pools_including_reserve > external_budget
                  OR pool_size < 0 OR reserve_pool_size < 0)
              THEN RAISE EXCEPTION 'pool plus reserve exceeds shared budget'; END IF;
            END $$;
            \\pset format unaligned
            \\pset tuples_only on
            SELECT json_build_object('sum', sum(pool_size)) FROM _pool_test;
        """
        sizes = []
        for demand in (4, 400):
            output = self.sql(source, variables=('connection_sessions_per_entry=0', 'pool_groups=2',
                                                'pooler_instances=2', f'cp1_peak_backend_demand={demand}'))
            sizes.append(next(json.loads(line)['sum'] for line in output.splitlines() if line.startswith('{"sum"')))
        self.assertLess(sizes[0], sizes[1])

    def test_missing_capability_stops_only_that_advisor(self):
        source = (ROOT / 'sql/capabilities.sql').read_text()
        source = source.replace("'S3', 'citus_shards'", "'S3', '__fixture_missing_citus_view'")
        output = self.sql('\\set advisor_id S3\n' + source + '\n\\echo unexpected execution after guard\n')
        self.assertIn('INCOMPLETE : advisor S3 unsupported', output)
        self.assertIn('__fixture_missing_citus_view', output)
        self.assertNotIn('unexpected execution after guard', output)

    def test_development_version_drift_is_detected(self):
        source = (ROOT / 'sql/advisors/v1_version_readiness.sql').read_text()
        source = source.replace("\\echo '-- V1a. Per-node PG & Citus versions --'", """
            UPDATE _v1_calc SET citus_lib=CASE WHEN groupid=0 THEN 'Citus 15.0devel on test'
                                             ELSE 'Citus 14.0devel on test' END;
        """)
        self.assertIn('CRITICAL : loaded Citus library versions differ', self.sql(source))

    def test_failed_probe_is_incomplete(self):
        source = (ROOT / 'sql/advisors/m1_node_memory_minimum.sql').read_text()
        source = source.replace('CREATE TEMP TABLE _m1_calc AS', """
            UPDATE _m1_raw SET success=false, result='unreachable' WHERE nodeid=(SELECT max(nodeid) FROM _m1_raw);
            CREATE TEMP TABLE _m1_calc AS
        """)
        self.assertIn('INCOMPLETE : probe', self.sql(source))

    def test_collector(self):
        output = self.sql(file=ROOT / 'sql/gather.sql')
        self.assertIn('bundle_version', output)
        self.assertIn('omitted', output)
        self.assertNotIn('repeat(', output)

    def test_full_driver(self):
        output_dir = Path(os.environ.get('CITUS_TEST_REPORT', str(self.root / 'report')))
        result = subprocess.run(['bash', str(ROOT / 'bin/citus_analyze'), '--psql', str(Path(BINDIR) / 'psql'),
                                 '-h', '127.0.0.1', '-p', str(self.nodes[0][1]), '-U', 'postgres', '-d', 'postgres',
                                 '-w', '--fail-on=none', '--output-format=html', '--out-dir', str(output_dir),
                                 '--coord-ram=2000.5MB', '--node-disk-size=1.5TB',
                                 '--advisor-var=m1_peak_connected=10', '--advisor-var=m1_peak_active=2',
                                 '--advisor-var=m1_capacity_active_pct=50', '--advisor-var=m1_growth_cache_pct=25',
                                 '--advisor-var=m1_growth_shard_mb=256',
                                 '--advisor-var=m1_internal_connections=4', '--advisor-var=m1_other_client_connections=2'],
                                capture_output=True, text=True, env=self.env, timeout=180)
        self.assertEqual(result.returncode, 0, result.stdout[-4000:] + result.stderr[-3000:])
        self.assertIn('assumed same as coordinator', result.stdout)
        self.assertIn('1572864', (output_dir / 'D1.out').read_text())
        self.assertEqual(len(list(output_dir.glob('*.json'))), 25)
        html = (output_dir / 'report.html').read_text()
        self.assertIn('Memory and room to grow', html)
        self.assertTrue('additional similar distributed table' in html, 'Expected a visible table-growth estimate')
        self.assertIsNotNone(json.loads((output_dir / 'M1.json').read_text())['analysis']['cluster_extra_shards'])
        self.assertIn('Application-client capacity', html)
        for node in json.loads((output_dir / 'M1.json').read_text())['analysis']['nodes']:
            self.assertEqual(node['ram_mib'], 2000.5)
            self.assertEqual(node['application_connection_limit'], max(0, node['connection_limit'] - 6))
        if os.environ.get('CITUS_TEST_REPORT'):
            print('Fixture report:', output_dir / 'report.html')
        for wrong in ('WILL OOM', 'ROLLBACK PREPARED each', 'RTT > 50', 'no HA configured', 'Cost model is lying'):
            self.assertNotIn(wrong, html)


if __name__ == '__main__':
    unittest.main()