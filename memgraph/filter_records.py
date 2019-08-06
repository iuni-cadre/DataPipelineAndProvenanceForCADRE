import sys
import os
import logging
import argparse
from config import MAG_DELIMITER

logging.basicConfig(level=logging.DEBUG)

SKIP_LINES = 0


def read_col(filepath, col, apply_fn=None, ignore_empty=False):
    with open(filepath, 'rb') as f:
        # skip headers
        for _ in range(SKIP_LINES):
            next(f)

        count = 0
        for line in f:
            count += 1

            row = line.split(MAG_DELIMITER)

            try:
                if row[col].strip() == b'' and not ignore_empty:
                    raise ValueError('Column {} is empty: {}'.format(col, row))
                elif row[col].strip() == b'' and ignore_empty:
                    continue
                else:
                    yield row[col] if apply_fn is None else apply_fn(row[col])
            except Exception as e:
                print('Row: {}'.format(row))
                print('Row length: {}'.format(len(row)))
                print('Count: {}'.format(count))
                print('Exception: {}'.format(e))
                exit(1)


def filter_records(src, dest, filter_cols, criteria, apply_fn):
    with open(src, 'rb') as srcf:
        with open(dest, 'wb') as destf:
            count = 0
            for line in srcf:
                try:
                    row = line.split(MAG_DELIMITER)
                    count += 1
                    include_row = True
                    for filter_col in filter_cols:
                        val = row[filter_col] if apply_fn is None else apply_fn(row[filter_col])
                        if val not in criteria:
                            include_row = False
                            break

                    if include_row:
                        destf.write(line)
                except Exception as e:
                    print('Row: {}'.format(row))
                    print('Row length: {}'.format(len(row)))
                    print('Count: {}'.format(count))
                    print('Exception: {}'.format(e))
                    exit(1)


def parse_cmd_args():
    parser = argparse.ArgumentParser(
        description=(''),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('src', type=str, help='')
    parser.add_argument('dest', type=str, help='')
    parser.add_argument('filter_cols', nargs='+', type=int, help='')
    parser.add_argument('-f', '--apply_fn', type=str, help='')
    parser.add_argument('-r', '--raw_criteria', type=str, help='')
    parser.add_argument('-t', '--criteria_src', type=str, help='')
    parser.add_argument('-k', '--criteria_val_col', type=int, default=0, help='')
    parser.add_argument('-i', '--ignore_empty_criteria', action='store_true', help='')

    args = parser.parse_args()

    # verify criteria
    if args.raw_criteria is None and args.criteria_src is None:
        print('You need to specify a criteria for filtering, either manually or from a file.')
        exit(1)
        
    if args.raw_criteria is not None and args.criteria_src is not None:
        print('You need to specify criteria either manually or from a file, not both.')
        exit(1)
        
    if args.criteria_src is not None and (not os.path.exists(args.criteria_src) or not os.path.isfile(args.criteria_src)):
        print('Invalid criteria file: {}'.format(args.criteria_src))
        exit(1)

    # verify src
    if not os.path.exists(args.src) and not os.path.isfile(args.src):
        print('Invalid input file: {}'.format(args.src))
        exit(1)

    # verify dest
    if os.path.exists(args.dest):
        print('Destination exists: {}'.format(args.dest))
        print('To be safe, we will not overwrite. Please, delete it manually if that is what you want to do.')
        exit(1)

    if not os.path.exists(os.path.dirname(args.dest)):
        os.makedirs(os.path.dirname(args.dest))

    # parse apply_fn
    apply_fn = None if args.apply_fn is None else getattr(__builtins__, args.apply_fn)

    # parse criteria
    logging.debug('Loading criteria.')
    if args.raw_criteria is None:
        criteria = set(read_col(args.criteria_src, args.criteria_val_col, apply_fn, args.ignore_empty_criteria))
    else:
        criteria = set([bytes(part.strip().lower(), 'utf-8') for part in args.raw_criteria.split(',')])

    return args.src, args.filter_cols, args.dest, criteria, apply_fn


if __name__ == '__main__':
    src, filter_cols, dest, criteria, apply_fn = parse_cmd_args()
    logging.debug('Criteria size: {}'.format(len(criteria)))
    logging.debug('Filtering.')
    filter_records(src, dest, filter_cols, criteria, apply_fn)
