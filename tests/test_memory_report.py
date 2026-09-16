import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from advisor_result import read_result
from memory_capacity import capacity_summary, parse_capacity, render_capacity


class MemoryReportTests(unittest.TestCase):
    def fixture(self):
        return {'schema_version': 1, 'all_nodes_measured': True, 'active_pct': 50,
                'headroom_pct': 20, 'cache_pct': 25, 'os_reserve_mib': 1024, 'os_reserve_pct': 10,
                'cluster_extra_shards': 12, 'cluster_extra_tables': 3,
                'nodes': [{'server': 'worker<1>:5432', 'role': 'Worker', 'database_mib': 8192, 'ram_mib': 16000,
                           'connection_limit': 60, 'extra_connections': 40, 'limiting_factor': 'memory budget',
                           'extra_shards': 12, 'extra_tables': 3, 'selected_connected': 20, 'selected_active': 10,
                           'current_tables': 2, 'current_shards': 8, 'new_shard_mib': 128, 'new_table_shards': 4}]}

    def test_structured_roundtrip(self):
        text = 'INFO : estimate\nM1_CAPACITY_JSON=' + json.dumps(self.fixture())
        self.assertEqual(parse_capacity(text), self.fixture())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'M1.out').write_text(text)
            self.assertEqual(read_result(root, 'M1')['analysis'], self.fixture())
            (root / 'M1.out').write_text('INFO : estimate\nM1_CAPACITY_JSON=broken')
            self.assertEqual(read_result(root, 'M1')['collection_status'], 'incomplete')

    def test_capacity_is_explained_and_escaped(self):
        output = render_capacity(self.fixture())
        self.assertIn('60 total database connections per server', output)
        self.assertIn('12 additional shards', output)
        self.assertIn('3 additional similar distributed tables', output)
        self.assertIn('worker&lt;1&gt;', output)
        self.assertIn('not an exact point', output)
        self.assertIn('Do not add these allowances together', output)

    def test_single_table_uses_singular_and_shows_growth_assumptions(self):
        result = self.fixture()
        result['cluster_extra_tables'] = 1
        output = render_capacity(result)
        self.assertIn('1 additional similar distributed table</strong>', output)
        self.assertIn('Growth assumptions:', output)
        self.assertIn('128.00 MiB', output)
        self.assertIn('Selected connections', output)

    def test_unknown_and_partial_never_show_limits(self):
        result = self.fixture()
        self.assertNotIn('12 additional shards', render_capacity(result, complete=False))
        result['nodes'][0]['connection_limit'] = None
        result['nodes'][0]['extra_connections'] = None
        result['cluster_extra_shards'] = None
        result['cluster_extra_tables'] = None
        self.assertIn('Growth not estimated', render_capacity(result))
        self.assertIn('Provide RAM', capacity_summary(result))