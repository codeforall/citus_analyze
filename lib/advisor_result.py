import argparse
import json
import re
from pathlib import Path

from memory_capacity import parse_capacity


LEVELS = ('CRITICAL', 'WARN', 'INCOMPLETE', 'INFO', 'OK')
CLASSES = dict(zip(LEVELS, ('crit', 'warn', 'unk', 'info', 'ok')))
ERROR = re.compile(r'(?:^|:\s)(?:ERROR|FATAL|PANIC):', re.M)


def findings(text):
    found = []
    for line in text.splitlines():
        cells = line.strip().strip('|').split('|')
        for index, cell in enumerate(cells):
            match = re.match(r'^\s*(CRITICAL|WARN|INCOMPLETE|INFO|NOTICE|OK)(?:\s*:\s*(.*)|\s*$)', cell)
            if match:
                level = 'INFO' if match[1] == 'NOTICE' else match[1]
                detail = (match[2] or ' | '.join(cells[index + 1:])).strip()
                if detail:
                    found.append({'severity': level, 'message': detail})
    return found


def worst_verdict(text):
    items = findings(text)
    for level in LEVELS:
        for item in items:
            if item['severity'] == level:
                return CLASSES[level], f'{level} : {item["message"]}'
    return 'unk', 'INCOMPLETE : no verdict headline'


def read_result(run_dir, advisor):
    run_dir = Path(run_dir)
    output = run_dir / f'{advisor}.out'
    error = run_dir / f'{advisor}.err'
    status = run_dir / f'{advisor}.status'
    text = output.read_text(errors='replace') if output.exists() else ''
    errors = error.read_text(errors='replace') if error.exists() else ''
    notes = []
    capacity = None
    if advisor == 'M1':
        try:
            capacity = parse_capacity(text)
        except (ValueError, TypeError):
            notes.append('memory capacity data could not be read')
    if not output.exists() or not text.strip():
        notes.append('missing or empty output')
    if status.exists() and status.read_text().strip() != '0':
        notes.append('psql execution failed')
    if ERROR.search(errors) or ERROR.search(text):
        notes.append('SQL/connection error; inspect restricted error log')
    items = findings(text)
    if any(item['severity'] == 'INCOMPLETE' for item in items):
        notes.append('node or measurement coverage incomplete')
    severity, headline = worst_verdict(text)
    if severity == 'unk' and not notes:
        notes.append('no supported verdict')
    if notes:
        if severity not in ('crit', 'warn'):
            severity, headline = 'unk', 'INCOMPLETE : ' + '; '.join(notes)
        else:
            headline += ' [collection incomplete]'
    return {'schema_version': 1, 'advisor': advisor, 'severity': severity,
            'headline': headline, 'collection_status': 'incomplete' if notes else 'complete',
            'notes': notes, 'findings': items, 'analysis': capacity}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_dir', type=Path)
    parser.add_argument('advisor')
    args = parser.parse_args()
    result = read_result(args.run_dir, args.advisor)
    (args.run_dir / f'{args.advisor}.json').write_text(json.dumps(result, indent=2) + '\n')
    label = next(level for level in LEVELS if CLASSES[level] == result['severity'])
    print(label + '\t' + result['collection_status'] + '\t' + result['headline'])


if __name__ == '__main__':
    main()