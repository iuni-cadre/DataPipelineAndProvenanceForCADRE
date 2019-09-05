import os
import json


def generate_rel_name(rel_type):
    return ''.join([p.title() for p in rel_type.split('_')])


if __name__ == '__main__':
    schema_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'common', 'schema.json')
    with open(schema_file, 'r') as f:
        schema = json.loads(f.read())

    mag_dir = os.path.join('/', 'tigergraph', 'data', 'mag')

    for node in schema['node_types']:
        filepath = os.path.join(mag_dir, node['raw_file'])
        assert os.path.exists(filepath) and os.path.isfile(filepath)

        print('USE GRAPH mag')
        print('BEGIN')
        print('CREATE LOADING JOB load_{}_vertex FOR GRAPH mag {{'.format(node['name']))
        print('DEFINE FILENAME {}_file="{}";'.format(node['name'].lower(), filepath))

        excluded_fields = set(node['foreign_keys']) if 'foreign_keys' in node else set()
        print('LOAD {}_file TO VERTEX {} VALUES ($0, {}) USING header="false", separator="\\t";'.format(
            node['name'].lower(),
            node['name'].lower(),
            ', '.join(['${}'.format(idx) for idx, name in list(enumerate(node['cols'])) if name not in excluded_fields])
        ))

        print('}')
        print('END')
        print('RUN LOADING JOB load_{}_vertex'.format(node['name']))

    for rel in schema['relationships']:
        rel_name = generate_rel_name(rel['type'])
        filepath = os.path.join(mag_dir, rel['src_file'])
        assert os.path.exists(filepath) and os.path.isfile(filepath)

        print('USE GRAPH mag')
        print('BEGIN')
        print('CREATE LOADING JOB load_{}_edge FOR GRAPH mag {{'.format(rel['type']))
        print('DEFINE FILENAME {}_file="{}";'.format(rel_name, filepath))
        print('LOAD {}_file TO EDGE {} VALUES (${}, ${}) USING header="false", separator="\\t";'.format(
            rel_name,
            '_'.join([p.lower() for p in rel['type'].split('_')]),
            rel['start_col'],
            rel['end_col']
        ))
        print('}')
        print('END')
        print('RUN LOADING JOB load_{}_edge'.format(rel['type']))
