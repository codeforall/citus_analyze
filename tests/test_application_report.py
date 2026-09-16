import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from memory_capacity import capacity_summary, render_application_capacity


class ApplicationReportTests(unittest.TestCase):
    def node(self, setting, state, cap, application):
        return {'server': 'worker<1>', 'connection_limit': 500, 'citus_client_setting': setting,
                'citus_client_limit_status': state, 'citus_client_limit': cap,
                'application_connection_limit': application, 'internal_connection_allowance': 100,
                'other_client_allowance': 3}

    def test_total_and_application_limits_remain_distinct(self):
        node = self.node('100', 'limited', 100, 97)
        result = {'all_nodes_measured': True, 'nodes': [node], 'active_pct': 100}
        summary = capacity_summary(result)
        self.assertIn('500 total database connections', summary)
        self.assertIn('Citus cap: 97 per server', summary)
        output = render_application_capacity([node])
        self.assertIn('Application-client limit', output)
        self.assertIn('worker&lt;1&gt;', output)

    def test_zero_disabled_and_unknown_are_distinct(self):
        blocked = render_application_capacity([self.node('0', 'blocked', 0, 0)])
        self.assertIn('<td>0</td>', blocked)
        self.assertNotIn('No Citus cap', blocked)
        disabled = render_application_capacity([self.node('-1', 'disabled', None, 397)])
        self.assertIn('No Citus cap', disabled)
        unknown = render_application_capacity([self.node(None, 'unknown', None, None)])
        self.assertIn('Application capacity not fully estimated', unknown)
        self.assertNotIn('No Citus cap', unknown)
        self.assertIn('Not estimated', unknown)