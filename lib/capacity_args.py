import argparse
from decimal import Decimal, localcontext
import re


CAPACITY_FIELDS = ('coord_ram', 'worker_ram', 'coord_disk', 'worker_disk')
BYTE_FACTORS = {'B': 1, 'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4}
MAX_BYTES = 2 ** 63 - 1


def parse_size_mib(value):
    if len(value) > 64:
        raise ValueError('size is too long')
    match = re.fullmatch(r'\s*([0-9]+(?:\.[0-9]+)?)\s*(B|[KMGT]I?B)?\s*', value, re.IGNORECASE)
    if not match:
        raise ValueError('use a non-negative size such as 32GB, 1.5GiB or 64MB; bare numbers mean MiB')
    unit = (match[2] or 'MB').upper().replace('IB', 'B')
    with localcontext() as context:
        context.prec = 100
        byte_count = Decimal(match[1]) * BYTE_FACTORS[unit]
        if byte_count > MAX_BYTES:
            raise ValueError('size exceeds the supported maximum of 2^63-1 bytes')
        if byte_count != byte_count.to_integral_value():
            raise ValueError('size must represent a whole number of bytes')
        mib = format(byte_count / BYTE_FACTORS['MB'], 'f')
    return mib.rstrip('0').rstrip('.') if '.' in mib else mib


def resolve_capacities(*, coord_ram=None, worker_ram=None, coord_disk=None, worker_disk=None,
                       node_ram=None, node_disk=None):
    values = dict(coord_ram=coord_ram, worker_ram=worker_ram, coord_disk=coord_disk,
                  worker_disk=worker_disk, node_ram=node_ram, node_disk=node_disk)
    normalized = {}
    for name, value in values.items():
        try:
            normalized[name] = parse_size_mib(value) if value is not None else None
        except ValueError as error:
            raise ValueError(f'--{name.replace("_", "-")}: {error}') from error
    resolved, sources = {}, {}
    for resource in ('ram', 'disk'):
        coordinator, worker, shared = f'coord_{resource}', f'worker_{resource}', f'node_{resource}'
        if normalized[coordinator] is not None:
            resolved[coordinator], sources[coordinator] = normalized[coordinator], 'explicit coordinator value'
        elif normalized[shared] is not None:
            resolved[coordinator], sources[coordinator] = normalized[shared], 'shared node value'
        else:
            resolved[coordinator], sources[coordinator] = '', 'not provided'
        if normalized[worker] is not None:
            resolved[worker], sources[worker] = normalized[worker], 'explicit worker value'
        elif normalized[shared] is not None:
            resolved[worker], sources[worker] = normalized[shared], 'shared node value'
        elif normalized[coordinator] is not None:
            resolved[worker], sources[worker] = normalized[coordinator], 'assumed same as coordinator'
        else:
            resolved[worker], sources[worker] = '', 'not provided'
    return resolved, sources


def main():
    parser = argparse.ArgumentParser(description='Normalize capacity options to MiB for the driver.')
    parser.add_argument('--coord-ram')
    parser.add_argument('--worker-ram')
    parser.add_argument('--coord-disk')
    parser.add_argument('--worker-disk')
    parser.add_argument('--node-ram')
    parser.add_argument('--node-disk-size', '--node-disk', dest='node_disk')
    arguments = {name: value or None for name, value in vars(parser.parse_args()).items()}
    try:
        resolved, sources = resolve_capacities(**arguments)
    except ValueError as error:
        parser.error(str(error))
    print('|'.join([resolved[name] for name in CAPACITY_FIELDS] + [sources[name] for name in CAPACITY_FIELDS]))


if __name__ == '__main__':
    main()