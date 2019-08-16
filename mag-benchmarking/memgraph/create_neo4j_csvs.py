import sys
import os
import json
import csv
from config import MAG_DELIMITER, NEO4J_DELIMITER

csv.field_size_limit(sys.maxsize)

DATA_TYPE_MAP = {
    'date': 'string'
}


def create_node_file(src, dest, cols, data_types, skip_cols, id_col, id_space, quote_fn=lambda s: s, node_label=None):
    if not os.path.exists(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest))

    skip_idxs = [] if skip_cols is None or len(skip_cols) == 0 else [cols.index(col) for col in skip_cols]
    assert (skip_cols is None and len(skip_idxs) == 0) or len(skip_cols) == len(skip_idxs)

    data_types = {field: dt if dt not in DATA_TYPE_MAP else DATA_TYPE_MAP[dt] for field, dt in data_types.items()}
    
    with open(src, 'r') as srcf:
        with open(dest, 'w') as destf:
            reader = csv.reader(srcf, delimiter=MAG_DELIMITER.decode('utf-8'), quoting=csv.QUOTE_NONE)
            writer = csv.writer(destf, delimiter=NEO4J_DELIMITER.decode('utf-8'), quotechar='"', quoting=csv.QUOTE_ALL)
            header = []
            for col in cols:
                if col == id_col:
                    assert skip_cols is None or col not in skip_cols
                    item = '{}:ID({})'.format(col, id_space)
                elif skip_cols is None or len(skip_cols) == 0 or col not in skip_cols:
                    item = '{}:{}'.format(col, data_types[col] if col in data_types else 'string')
                else:
                    continue
                header.append(item)

            if node_label is not None:
                header.append(':LABEL')                
            writer.writerow(header)
            
            for row in reader:
                if skip_cols is not None and len(skip_cols) != 0 and len(skip_idxs) != 0:
                    parts = [i for j, i in enumerate(row) if j not in skip_idxs]
                else:
                    parts = row

                if node_label is not None:
                    parts.append(node_label)

                if len(parts) == len(header):
                    writer.writerow(parts)
                else:
                    print('Could not process row: {} ({} items)'.format('|'.join(parts), len(parts)))
                    print('Header: {} ({} items)'.format('|'.join(header), len(header)))
                    exit(1)


def create_relationship_file(src, dest, rel_type, start_col, end_col, start_id_space, end_id_space, start_apply_fn=None, end_apply_fn=None, quote_fn=lambda s: s):
    if not os.path.exists(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest))

    with open(src, 'rb') as srcf:
        with open(dest, 'wb') as destf:
            destf.write(bytes(':START_ID({}){}:END_ID({}){}:TYPE\n'.format(
                start_id_space,
                NEO4J_DELIMITER.decode('utf-8'),
                end_id_space,
                NEO4J_DELIMITER.decode('utf-8')
            ), 'utf-8'))
            line_count = 0
            for line in srcf:
                line_count += 1
                parts = [p.strip() for p in line.split(MAG_DELIMITER)]
                if parts[start_col] != b'' and parts[end_col] != b'':
                    destf.write(bytes('{}{}{}{}{}\n'.format(
                        quote_fn(parts[start_col] if start_apply_fn is None else start_apply_fn(parts[start_col])),
                        NEO4J_DELIMITER.decode('utf-8'),
                        quote_fn(parts[end_col] if end_apply_fn is None else end_apply_fn(parts[end_col])),
                        NEO4J_DELIMITER.decode('utf-8'),
                        quote_fn(rel_type)
                    ), 'utf-8'))


def create_id_space(name):
    return '{}_ID'.format(name.upper())


def process_all_files(mag_dir, neo4j_data_dir, schema):

    for node_type in schema['node_types']:
        print('Creating nodes: {}'.format(node_type['name']))
        create_node_file(
            os.path.join(mag_dir, node_type['raw_file']),
            os.path.join(neo4j_data_dir, 'nodes', '{}.csv'.format(node_type['name'])),
            node_type['cols'],
            node_type['data_types'],
            None if 'foreign_keys' not in node_type else node_type['foreign_keys'],
            node_type['id'],
            create_id_space(node_type['name']),
            quote_fn=lambda s: bytes('"{}"'.format(s.decode('utf-8')), 'utf-8'),
            node_label=node_type['name'].upper()
        )

    for rel in schema['relationships']:
        basename = '{}-{}-{}'.format(rel['start_node'], rel['type'].title().replace('_', ''), rel['end_node'])
        print('Creating relationships: {}'.format(basename))
        create_relationship_file(
            os.path.join(mag_dir, rel['src_file']),
            os.path.join(neo4j_data_dir, 'relationships', '{}.csv'.format(basename)),
            rel['type'],
            rel['start_col'],
            rel['end_col'],
            create_id_space(rel['start_node']),
            create_id_space(rel['end_node']),
            int, int
        )


def get_node(node_name, schema):
    for node in schema['node_types']:
        if node['name'] == node_name:
            return node


if __name__ == '__main__':
    mag_dir = os.path.join('/', 'raw', 'reduced-mags', 'mag-fos-computer-science')
    neo4j_data_dir = os.path.join('/', 'raw', 'neo4j-import', 'mag-fos-computer-science')
    schema_file = 'schema.json'

    with open(schema_file, 'r') as f:
        schema = json.loads(f.read())

    process_all_files(mag_dir, neo4j_data_dir, schema)
