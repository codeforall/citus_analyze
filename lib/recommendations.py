import re

from advisor_result import findings


_GUIDANCE = {
    'M1': ('Memory and room to grow', 'Estimates memory use, total database connections, a separate application-client limit, and room for more tables or shards (pieces of a table).', 'Internal Citus work also needs connections. The application estimate respects the Citus client cap and the stated allowances; neither estimate guarantees performance.', 'Provide peak internal and other-client allowances. Check both capacity limits and test at peak load before increasing connections or adding data.'),
    'D1': ('Storage space and growth', 'Shows database size and estimates when storage may fill up, using the free space and growth rate you provide.', 'Database size is only part of the disk space in use. Files, logs and other databases also need space.', 'Provide recent free-space measurements and a growth rate for each server before planning a storage increase.'),
    'Q1': ('Slow work and waiting queries', 'Finds long-running requests and requests waiting for other work to finish.', 'Some long requests are expected. Stopping them can interrupt the application.', 'Ask the application owner which requests are expected before stopping any work.'),
    'GUC1': ('Database settings', 'Checks server settings against the selected rules and shows differences between servers.', 'Servers with different roles or sizes may need different settings.', 'Review each warning. For transaction capacity, compare the available slots with peak demand before changing settings.'),
    'V1': ('Software versions', 'Shows PostgreSQL and Citus versions, installed extensions, and available update files.', 'Installed update files may differ from the software currently running.', 'Check which versions work together and follow the documented update procedure.'),
    'P1': ('Space for time-based data', 'Checks whether date-based table sections cover expected time ranges, including any catch-all section.', 'Missing time ranges matter only when new data or older records need to be written there.', 'Compare the uncovered dates with planned data loading before creating more sections.'),
    'I1': ('Indexes and search performance', 'Checks indexes, the structures that help find rows quickly, for errors, use and possible duplication.', 'An index with no recent use may still support important or infrequent work.', 'Check application needs and index dependencies before rebuilding or removing an index.'),
    'B1': ('Table cleanup', 'Estimates rows waiting for cleanup and shows current automatic cleanup activity.', 'Rows waiting for cleanup do not directly measure wasted disk space.', 'Check cleanup progress, blocked work and disk activity over time before changing cleanup settings.'),
    'CP1': ('Connection pool sizing', 'Estimates how many connections PgBouncer can share, including spare connections and all pooler instances.', 'Every pool uses the same limited server capacity.', 'Provide peak demand and the number of database/user pools. Check application compatibility before changing pooling mode.'),
    'R2': ('Planned data moves', 'Shows which data the current rebalance plan would move and estimates the transfer work.', 'No planned moves does not mean a perfect balance. Transfer times do not include every part of the job.', 'Check the plan, available destination storage and expected load before starting data moves.'),
    'REF1': ('Shared table copies', 'Checks copies of reference tables, which are stored on multiple servers, and compares their sizes.', 'Different file sizes do not necessarily mean the rows differ.', 'Check which servers need a copy and verify the actual data before repairing copies.'),
    'W1': ('Write logs and data protection', 'Checks recovery-log activity, disk-write checkpoints, archiving and data-protection settings.', 'Old failures or quiet periods do not prove a current problem.', 'Check recent errors and log buildup before changing settings. Keep crash-protection features enabled.'),
    'STAT1': ('Information used to plan queries', 'Checks the row estimates PostgreSQL uses to choose how to run a query.', 'Old estimates may still be useful when the data has not changed.', 'Refresh missing or outdated estimates on changed tables, then check whether query plans improve.'),
    'SEC1': ('Access and security checks', 'Shows users, permissions, password methods and connection access rules.', 'Some security checks need extra permission. This report is not a full security review.', 'Confirm that access matches your policy and review any checks that could not run.'),
    'NET1': ('Connections between servers', 'Checks whether servers can reach each other and measures the time for a complete group of probes.', 'The total probe time is not the network delay of each server.', 'Investigate failed connections. Measure each network path separately before diagnosing slow networking.'),
    'REP1': ('Replication and retained logs', 'Shows visible data-copy connections and logs kept for replication consumers.', 'No visible connection does not prove backups or high availability are absent.', 'Check replication progress and free storage. Confirm who needs retained logs before changing retention.'),
    'GR1': ('Memory needed for growth', 'Estimates the extra tracking information and locks needed for more tables or shards.', 'Memory use depends on the workload, not only on the number of tables.', 'Test the proposed growth with representative queries and check memory on every server.'),
    'C3': ('Database connection capacity', 'Estimates direct connections and connections opened between servers for distributed work, including the Citus client cap.', 'One application request can use several database connections. A free PostgreSQL slot may still be reserved for internal work.', 'Check the Citus client cap and measure peak demand across all servers, including connections kept open for reuse.'),
    'S3': ('How evenly data is stored', 'Compares pieces of the same table and the amount of data stored on each server.', 'Uneven size is not always uneven workload. Larger servers may intentionally hold more data.', 'Compare data sizes with server capacity and actual query load before moving data.'),
    'R1': ('Background jobs', 'Shows active job retries, recent failures and data-move progress.', 'A long-running job is not necessarily stuck.', 'Compare progress over time and read current errors before restarting or canceling a job.'),
    'N6': ('Planning to add a server', 'Estimates the work needed to copy cluster setup information to a new server.', 'This estimate cannot confirm that adding the server will succeed.', 'Check the new server resources and supported features. Keep the default consistency mode unless a diagnosed problem requires a change.'),
    'A3': ('Transactions waiting to finish', 'Checks transactions waiting for a final decision, their age and available transaction slots.', 'A missing recovery record does not tell us whether to complete or cancel a transaction.', 'Ask a database operator to check the originating server and recovery state. Do not resolve transactions from this report alone.'),
    'SC1': ('Number of table pieces', 'Compares shard counts (pieces of a table) with data size and the selected size target.', 'The right shard count depends on query load and future growth.', 'Test the proposed shard count and expected disruption before changing table layout.'),
    'P2': ('Recorded and actual data sizes', 'Compares recorded shard sizes with their current sizes on disk.', 'A difference does not by itself mean the rebalance plan is wrong.', 'Check how the installed rebalance method uses size information before taking action.'),
    'MX1': ('Connections from other servers', 'Estimates connections arriving from other Citus servers and room left for direct clients.', 'Several servers may send work to the same destination at once.', 'Compare expected incoming work with peak measurements and leave space for direct client connections.'),
}
GUIDANCE = {advisor: dict(zip(('title', 'checks', 'matters', 'fix'), values))
            for advisor, values in _GUIDANCE.items()}


def recommendation(advisor, text):
    if not text.strip():
        return 'Not assessed: no advisor output.'
    if advisor == 'GUC1':
        critical = re.search(r'(\d+)\s+rule violation\(s\);\s*(\d+)\s+(?:(?:must-match GUC|critical)\s+)?drift\(s\);\s*(\d+)\s+warning', text)
        if critical:
            return f'Current: {critical[1]} serious setting issues; {critical[2]} serious differences between servers; {critical[3]} warnings. ' + GUIDANCE[advisor]['fix']
        warnings = re.search(r'(\d+)\s+rule warning\(s\);\s*(\d+)\s+cross-node drift', text)
        if warnings:
            return f'Current: {warnings[1]} setting warnings; {warnings[2]} differences between servers. ' + GUIDANCE[advisor]['fix']
    if advisor == 'M1' and 'RECOMMENDED NODE RAM' in text:
        return 'Legacy memory calculation: rerun the corrected per-node scenario advisor before sizing.'
    items = findings(text)
    if not items:
        return 'Not assessed: no supported finding parsed. ' + GUIDANCE[advisor]['fix']
    return GUIDANCE[advisor]['fix']


_ATTENTION = {
    'M1': 'The estimated workload needs more memory than the amount provided.',
    'D1': 'The storage measurements cross a selected free-space or growth limit.',
    'Q1': 'Some requests are taking a long time or waiting for other work.',
    'GUC1': 'Some database settings need review.',
    'V1': 'Some software versions or update steps need review.',
    'P1': 'Some table sections or date ranges need review.',
    'I1': 'Some indexes need a closer check before changes are made.',
    'B1': 'Some tables may need more cleanup attention.',
    'CP1': 'The requested connection pools may exceed their assigned budget.',
    'R2': 'The data-move plan needs a storage or workload review.',
    'REF1': 'Some shared table copies or their sizes need review.',
    'W1': 'Some write-log or data-protection checks need attention.',
    'STAT1': 'Some query-planning estimates may need to be refreshed.',
    'SEC1': 'Some access rules or permissions need a security review.',
    'NET1': 'Some servers could not reach each other during the check.',
    'REP1': 'Replication progress, retained logs or data-protection settings need review.',
    'GR1': 'The proposed growth needs a closer memory review.',
    'C3': 'The estimated connections exceed at least one server or pool limit.',
    'S3': 'Some pieces of the same table are larger than the selected balance limit.',
    'R1': 'Some current or recent background jobs need review.',
    'N6': 'The new-server estimate exceeds a selected resource limit.',
    'A3': 'Some transactions have waited too long or used much of the available capacity.',
    'SC1': 'Some shard counts differ from the selected size target.',
    'P2': 'Some recorded sizes differ from sizes on disk; the cause still needs checking.',
    'MX1': 'Estimated connections from other servers exceed a destination limit.',
}


def plain_summary(advisor, result):
    severity = result['severity']
    if severity == 'unk':
        return 'Not fully checked. Some data is missing, access was denied, or this feature is not supported.'
    if advisor == 'GUC1' and severity in ('warn', 'crit'):
        text = recommendation(advisor, result['headline'])
        message = text.split('. ', 1)[0] + '.' if text.startswith('Current:') else _ATTENTION[advisor]
    elif severity in ('warn', 'crit'):
        message = _ATTENTION[advisor]
    elif severity == 'ok':
        message = 'No issue found in the checks that ran.'
    else:
        message = 'Information or planning estimate. This result alone does not indicate a fault.'
    if result.get('collection_status') == 'incomplete':
        message += ' Some checks could not finish; the result is partial.'
    return message