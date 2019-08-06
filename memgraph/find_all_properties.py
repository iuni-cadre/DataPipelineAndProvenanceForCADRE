import json


if __name__ == '__main__':
    with open('schema.json', 'r') as f:
        schema = json.loads(f.read())

    all_cols = set()
    for node_type in schema['node_types']:
        cols = node_type['cols']
        cols.remove(node_type['id'])
        all_cols.update(cols)

    print(','.join(sorted(all_cols)))
