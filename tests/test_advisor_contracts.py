from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class AdvisorContracts(unittest.TestCase):
    def test_a3_never_decides_manual_recovery(self):
        source = (ROOT / 'sql/advisors/a3_2pc_backlog_advisor.sql').read_text()
        self.assertNotIn('ROLLBACK PREPARED', source)
        self.assertNotIn('will NOT auto-recover', source)
        self.assertIn('database <> current_database()', source)
        self.assertIn('pg_dist_local_group', source)
        self.assertIn('WHERE NOT success', source)
        self.assertIn('jsonb_array_elements', source)

    def test_memory_is_per_node_and_scenario_based(self):
        source = (ROOT / 'sql/advisors/m1_node_memory_minimum.sql').read_text()
        self.assertIn('run_command_on_all_nodes', source)
        self.assertIn("current_setting('autovacuum_work_mem')", source)
        self.assertIn('least(active *', source)
        self.assertNotIn('WILL OOM', source)
        self.assertNotIn('RECOMMENDED NODE RAM', source)


if __name__ == '__main__':
    unittest.main()