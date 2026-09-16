import unittest

import test_sql


@unittest.skipUnless(test_sql.BINDIR, 'Set CITUS_TEST_BINDIR for client-limit SQL tests')
class ClientLimitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_sql.SqlTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        test_sql.SqlTests.tearDownClass()

    def advisor_source(self, filename, client_setting):
        shared = (test_sql.ROOT / 'sql/connection_budget.sql').read_text()
        shared = shared.replace('CREATE TEMP TABLE _connection_nodes AS', f"""
            UPDATE _connections_raw SET result=(result::jsonb ||
              jsonb_build_object('max_client_connections', {client_setting}))::text WHERE success;
            CREATE TEMP TABLE _connection_nodes AS
        """).replace('\\ir advisor_coverage.sql', '\\ir ../advisor_coverage.sql').replace('\\ir client_limit.sql', '\\ir ../client_limit.sql')
        return (test_sql.ROOT / 'sql/advisors' / filename).read_text().replace('\\ir ../connection_budget.sql', shared)

    def test_positive_cap_warns_in_every_connection_advisor(self):
        for filename in ('c3_max_external_connections.sql', 'mx1_mesh_connection_budget.sql', 'cp1_pgbouncer_pool.sql'):
            with self.subTest(advisor=filename):
                output = test_sql.SqlTests.sql(self.advisor_source(filename, "'5'"),
                                               variables=('connection_sessions_per_entry=10', 'connections_per_session=0', 'pool_groups=2'))
                self.assertIn('WARN : proposed', output)
                self.assertIn('external connection budget', output)

    def test_resolver_distinguishes_zero_disabled_and_unknown(self):
        output = test_sql.SqlTests.sql("""
            \\ir ../client_limit.sql
            DO $$ DECLARE resolved record; configured text; BEGIN
              FOR configured IN SELECT unnest(ARRAY['0','5','-1',NULL,'','auto','-2','2147483648']) LOOP
                SELECT * INTO resolved FROM pg_temp.citus_client_limit(configured);
                IF configured='0' AND (resolved.client_limit IS DISTINCT FROM 0 OR resolved.client_limit_status <> 'blocked') THEN
                  RAISE EXCEPTION 'zero must block regular clients'; END IF;
                IF configured='5' AND (resolved.client_limit IS DISTINCT FROM 5 OR resolved.client_limit_status <> 'limited') THEN
                  RAISE EXCEPTION 'positive cap lost'; END IF;
                IF configured='-1' AND (resolved.client_limit IS NOT NULL OR resolved.client_limit_status <> 'disabled') THEN
                  RAISE EXCEPTION 'disabled must remain distinct from unknown'; END IF;
                IF (configured IS NULL OR configured NOT IN ('0','5','-1')) AND
                    (resolved.client_limit IS NOT NULL OR resolved.client_limit_status <> 'unknown') THEN
                  RAISE EXCEPTION 'unrecognized value treated as unlimited'; END IF;
              END LOOP;
            END $$;
        """)
        self.assertIn('DO', output)

    def test_unknown_budgets_do_not_become_pool_sizes(self):
        source = self.advisor_source('cp1_pgbouncer_pool.sql', 'NULL')
        source = source.replace('WITH budgets AS (', 'CREATE TEMP TABLE _client_test AS WITH budgets AS (')
        source += """
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM _client_test WHERE external_budget IS NOT NULL OR pool_size IS NOT NULL
                         OR reserve_pool_size IS NOT NULL OR all_pools_including_reserve IS NOT NULL)
              THEN RAISE EXCEPTION 'unknown client cap produced numeric pool recommendation'; END IF;
            END $$;
        """
        self.assertIn('INCOMPLETE', test_sql.SqlTests.sql(source, variables=('pool_groups=2',)))

    def test_zero_budget_in_every_connection_advisor(self):
        for filename in ('c3_max_external_connections.sql', 'mx1_mesh_connection_budget.sql', 'cp1_pgbouncer_pool.sql'):
            with self.subTest(advisor=filename):
                source = self.advisor_source(filename, "'0'")
                source = source.replace('DROP TABLE pg_temp._connection_budget;', """
                    DO $$ BEGIN
                      IF EXISTS (SELECT 1 FROM _connection_budget WHERE external_budget IS DISTINCT FROM 0)
                      THEN RAISE EXCEPTION 'zero cap did not set external budget to zero'; END IF;
                    END $$;
                    DROP TABLE pg_temp._connection_budget;
                """)
                output = test_sql.SqlTests.sql(source, variables=('connection_sessions_per_entry=1',))
                self.assertIn('WARN : proposed', output)


if __name__ == '__main__':
    unittest.main()