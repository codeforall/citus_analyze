import html
import json
import math


PREFIX = 'M1_CAPACITY_JSON='


def parse_capacity(text):
    records = [line[len(PREFIX):] for line in text.splitlines() if line.startswith(PREFIX)]
    if not records:
        return None
    if len(records) != 1:
        raise ValueError('Expected one memory capacity record')
    result = json.loads(records[0])
    if not isinstance(result, dict) or result.get('schema_version') != 1 or not isinstance(result.get('nodes'), list):
        raise ValueError('Unsupported memory capacity record')
    for node in result['nodes']:
        if not isinstance(node, dict) or not isinstance(node.get('server'), str):
            raise ValueError('Invalid memory capacity node')
    return result


def _number(value, digits=0):
    if value is None or not isinstance(value, (int, float)) or not math.isfinite(value):
        return 'Not estimated'
    return f'{value:,.{digits}f}'


def capacity_summary(result):
    if not result or not result.get('all_nodes_measured'):
        return 'More information is needed to estimate memory capacity.'
    limits = [node['connection_limit'] for node in result['nodes'] if node.get('connection_limit') is not None]
    if len(limits) != len(result['nodes']) or not limits:
        return 'Provide RAM and valid workload inputs for every server to estimate capacity.'
    span = _number(min(limits)) if min(limits) == max(limits) else f'{_number(min(limits))} to {_number(max(limits))}'
    application_limits = [node['application_connection_limit'] for node in result['nodes']
                          if node.get('application_connection_limit') is not None]
    if len(application_limits) == len(result['nodes']):
        application_span = _number(min(application_limits)) if min(application_limits) == max(application_limits) else f'{_number(min(application_limits))} to {_number(max(application_limits))}'
        application = f'Application-client limit after allowances and the Citus cap: {application_span} per server.'
    else:
        application = 'Application-client capacity is not fully assessed; see the separate limits below.'
    return (f'Memory-only planning limit: {span} total database connections per server, with {_number(result.get("active_pct"))}% busy at once. '
            f'{application} Confirm both with a load test.')


def render_application_capacity(nodes):
    parts = ['<h3>Application-client capacity</h3>',
             '<p>This is a separate limit for regular (non-superuser) application connections. '
             'It takes the lower of the Citus client cap and the database capacity left after internal work and other clients. '
             'The Citus cap is shared across all databases on each server.</p>',
             '<div class="scroll"><table class="memory-table"><thead><tr><th>Server</th>'
             '<th>Citus setting</th><th>Effective client cap</th><th>Internal allowance</th>'
             '<th>Other-client allowance</th><th>Application-client limit</th></tr></thead><tbody>']
    for node in nodes:
        state = node.get('citus_client_limit_status', 'unknown')
        effective = 'No Citus cap' if state == 'disabled' else _number(node.get('citus_client_limit')) if state in ('limited', 'blocked') else 'Unknown'
        columns = [node['server'], str(node.get('citus_client_setting')) if node.get('citus_client_setting') is not None else 'Unknown',
                   effective, _number(node.get('internal_connection_allowance')),
                   _number(node.get('other_client_allowance')), _number(node.get('application_connection_limit'))]
        parts.append('<tr>' + ''.join(f'<td>{html.escape(value)}</td>' for value in columns) + '</tr>')
    parts.append('</tbody></table></div>')
    if any(node.get('application_connection_limit') is None for node in nodes):
        parts.append('<p><strong>Application capacity not fully estimated.</strong> Provide a whole-number peak internal-connection allowance '
                     'and resolve missing or unrecognized Citus settings. Older bundles need a new memory check.</p>')
    parts.append('<p>For <code>citus.max_client_connections</code>, <strong>0 blocks regular clients</strong> and '
                 '<strong>-1 disables the Citus cap</strong>; neither means an automatic client limit. '
                 'Superusers are exempt from rejection, but external administrative sessions still count toward the client total. '
                 'Use <code>m1_internal_connections</code> for peak internal work and <code>m1_other_client_connections</code> '
                 'for connections outside this application. Internal work can grow with application traffic: validate the allowance '
                 'against C3/MX1 and peak load. These are total application limits, not additional clients.</p>')
    return '\n'.join(parts)


def render_capacity(result, complete=True):
    parts = ['<section class="memory-plan" id="memory-planning" aria-labelledby="memory-heading">',
             '<h2 class="section" id="memory-heading">Memory and room to grow</h2>']
    if not complete or not result or not result.get('all_nodes_measured'):
        parts.append('<p>Capacity was not calculated from complete data. Run the memory check with RAM values for every server and resolve any failed checks.</p></section>')
        return '\n'.join(parts)
    nodes = result['nodes']
    parts.append(f'<p><strong>{html.escape(capacity_summary(result))}</strong></p>')
    parts.append('<p>These are database connections, including internal Citus work, not application users. '
                 'Data size on disk is shown for context: PostgreSQL does not need to keep all data in RAM.</p>')
    parts.append('<div class="scroll"><table class="memory-table"><thead><tr>'
                 '<th>Server</th><th>Database on disk</th><th>Provided RAM</th>'
                 '<th>Selected connections</th><th>Estimated total limit</th><th>Additional connections</th><th>Limit reached first</th>'
                 '</tr></thead><tbody>')
    for node in nodes:
        disk = _number(node.get('database_mib') / 1024, 2) + ' GiB' if node.get('database_mib') is not None else 'Unknown'
        ram = _number(node.get('ram_mib') / 1024, 2) + ' GiB' if node.get('ram_mib', 0) > 0 else 'Not provided'
        columns = [f'{node.get("role", "Server")} {node["server"]}', disk, ram, _number(node.get('selected_connected')),
                   _number(node.get('connection_limit')), _number(node.get('extra_connections')),
                   node.get('limiting_factor', 'Unknown') if node.get('connection_limit') is not None else 'Not estimated']
        parts.append('<tr>' + ''.join(f'<td>{html.escape(str(value))}</td>' for value in columns) + '</tr>')
    parts.append('</tbody></table></div>')
    parts.append(f'<p>Planning buffer: <strong>{_number(result.get("headroom_pct"))}% of provided RAM</strong> is left unused, '
                 f'plus the operating-system reserve (the larger of {_number(result.get("os_reserve_mib"))} MiB or '
                 f'{_number(result.get("os_reserve_pct"))}% of estimated use). '
                 'The total limit includes existing connections. The additional count is the room above the selected workload.</p>')
    if result.get('cache_pct') is None:
        parts.append('<p>No separate data-cache target was provided. The connection estimate includes configured shared memory and the other stated allowances, '
                     'but unmeasured file-cache or application memory needs may reduce it.</p>')
    parts.append(render_application_capacity(nodes))
    parts.append('<h3>More tables or shards with the same workload</h3>')
    shards, tables = result.get('cluster_extra_shards'), result.get('cluster_extra_tables')
    if shards is None or tables is None:
        parts.append('<p><strong>Growth not estimated.</strong> Provide peak connected and busy connection counts, '
                     'the percentage of distributed data to keep cached, and data sizes. An empty cluster needs a representative workload first.</p>')
    else:
        shard_word = 'shard' if shards == 1 else 'shards'
        table_word = 'table' if tables == 1 else 'tables'
        parts.append(f'<p>Across the cluster, the memory calculation leaves room for approximately <strong>{_number(shards)} additional {shard_word}</strong> '
                     f'or <strong>{_number(tables)} additional similar distributed {table_word}</strong>. '
                     'A shard is one piece of a distributed table. The server with the least room sets this estimate.</p>')
        sample = nodes[0]
        parts.append(f'<p>Growth assumptions: each new shard reaches <strong>{_number(sample.get("new_shard_mib"), 2)} MiB</strong>; '
                     f'each new table has <strong>{_number(sample.get("new_table_shards"))} shards</strong>; '
                     f'<strong>{_number(result.get("cache_pct"))}% of distributed data</strong> is kept cached in memory. '
                     'Connection counts and query complexity stay fixed. Review these assumptions before using the estimate.</p>')
        bottlenecks = [node['server'] for node in nodes if node.get('extra_shards') == shards]
        parts.append(f'<p>Shard-growth limit set by: {html.escape(", ".join(bottlenecks))}.</p>')
    parts.append('<p><strong>Choose one change at a time.</strong> More connections and more data use the same memory. '
                 'Do not add these allowances together. CPU, storage, locks or connections between servers may limit growth sooner. '
                 'These figures are not an exact point where the server runs out of memory.</p>')
    parts.append('<details><summary>Workload and growth assumptions</summary>')
    parts.append('<p>Growth keeps the selected connections and query complexity fixed. New shards use the stated size, '
                 'current average index count and current placement pattern. Extra cache space is budgeted separately for new data. '
                 'A new table also needs its own tracking information.</p>')
    parts.append(f'<p>Data-cache target: {_number(result.get("cache_pct"))}% of distributed data. '
                 'This is a user assumption, not a measured working set.</p>')
    parts.append('<div class="scroll"><table class="memory-table"><thead><tr><th>Server</th><th>Selected connections</th>'
                 '<th>Busy connections</th><th>Current cluster tables / shards</th><th>Size per new shard</th>'
                 '<th>Shards per new table</th></tr></thead><tbody>')
    for node in nodes:
        columns = [node['server'], _number(node.get('selected_connected')), _number(node.get('selected_active')),
                   f'{_number(node.get("current_tables"))} / {_number(node.get("current_shards"))}',
                   f'{_number(node.get("new_shard_mib"), 2)} MiB', _number(node.get('new_table_shards'))]
        parts.append('<tr>' + ''.join(f'<td>{html.escape(value)}</td>' for value in columns) + '</tr>')
    parts.append('</tbody></table></div></details></section>')
    return '\n'.join(parts)