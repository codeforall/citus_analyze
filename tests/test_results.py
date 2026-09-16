import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from advisor_result import read_result, worst_verdict
from render_html import render
from recommendations import GUIDANCE, plain_summary, recommendation


class ResultTests(unittest.TestCase):
    def test_guc_counts_are_current_first(self):
        result = recommendation('GUC1', 'WARN : 4 rule warning(s); 0 cross-node drift(s).')
        self.assertTrue(result.startswith('Current: 4 setting warnings; 0 differences between servers'))
        for prefix in ('must-match GUC ', 'critical ', ''):
            result = recommendation('GUC1', f'CRITICAL : 2 rule violation(s); 1 {prefix}drift(s); 3 warning(s).')
            self.assertIn('Current: 2 serious setting issues; 1 serious differences between servers; 3 warnings', result)

    def test_plain_summaries_keep_partial_results_visible(self):
        for advisor in GUIDANCE:
            message = plain_summary(advisor, {'severity': 'warn', 'headline': 'WARN : issue', 'collection_status': 'incomplete'})
            self.assertIn('result is partial', message)
            self.assertNotIn('No issue found', message)
        self.assertIn('Not fully checked', plain_summary('M1', {'severity': 'unk'}))

    def test_no_healthy_or_destructive_fallbacks(self):
        for advisor in ('M1', 'A3', 'N6', 'SC1', 'REP1', 'SEC1'):
            self.assertIn('Not assessed', recommendation(advisor, ''))
        self.assertIn('Legacy memory calculation', recommendation('M1', 'RECOMMENDED NODE RAM : 238427 MB ~ 232.84 GB'))

    def test_table_cells_and_notice(self):
        self.assertEqual(worst_verdict('| node1 | CRITICAL: bad |\nOK : fine')[0], 'crit')
        self.assertEqual(worst_verdict('| WARN | R2 | capacity |')[0], 'warn')
        self.assertEqual(worst_verdict('NOTICE : moves planned')[0], 'info')
        self.assertEqual(worst_verdict('untrusted SQL contains CRITICAL: literal')[0], 'unk')

    def test_failed_execution_preserves_known_critical(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'M1.out').write_text('CRITICAL : known issue')
            (root / 'M1.status').write_text('3')
            result = read_result(root, 'M1')
            self.assertEqual(result['severity'], 'crit')
            self.assertEqual(result['collection_status'], 'incomplete')

    def test_errors_never_become_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'M1.out').write_text('OK : fine')
            (root / 'M1.err').write_text('psql:advisor.sql:10: ERROR: permission denied')
            self.assertEqual(read_result(root, 'M1')['severity'], 'unk')
            self.assertEqual(read_result(root, 'A3')['severity'], 'unk')
            (root / 'M1.err').write_text('NOTICE: temporary table does not exist, skipping')
            self.assertEqual(read_result(root, 'M1')['severity'], 'ok')

    def test_empty_bundle_is_prominently_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            render(root, root / 'report.html')
            html = (root / 'report.html').read_text()
            self.assertIn('Collection incomplete:', html)
            self.assertIn('25 incomplete', html)
            self.assertNotIn('Nothing urgent', html)


if __name__ == '__main__':
    unittest.main()