import os
import json

# mappings from schema.json to GSQL
dtype_mappings = {
    'long': 'INT',
    'date': 'DATETIME',
    'int': 'INT'
}


def convert_dtype(dt):
    if dt in dtype_mappings:
        return dtype_mappings[dt]
    else:
        raise ValueError('Invalid data type: {}'.format(dt))


if __name__ == '__main__':
    schema_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'common', 'schema.json')
    with open(schema_file, 'r') as f:
        schema = json.loads(f.read())

    vertices = []
    edges = []
    for node in schema['node_types']:
        excluded_fields = set(node['foreign_keys']) if 'foreign_keys' in node else set()
        data_defs = 'PRIMARY_ID {} {}'.format(
            node['id'],
            convert_dtype(node['data_types'][node['id']]) if node['id'] in node['data_types'] else 'STRING'
        )
        for field in node['cols']:
            if field not in excluded_fields and field != node['id']:
                data_defs += ', {} {}'.format(
                    field,
                    convert_dtype(node['data_types'][field]) if field in node['data_types'] else 'STRING'
                )
        gsql = 'CREATE VERTEX {} ({}) WITH primary_id_as_attribute="true"'.format(
            node['name'].lower(),
            data_defs
        )
        vertices.append(node['name'].lower())
        print(gsql)

    for rel in schema['relationships']:
        gsql = 'CREATE DIRECTED EDGE {} (FROM {}, TO {})'.format(
            rel['type'].lower(),
            rel['start_node'].lower(),
            rel['end_node'].lower()
        )
        edges.append(rel['type'].lower())
        print(gsql)
        
    print('CREATE GRAPH mag ({}, {})'.format(', '.join(vertices), ', '.join(edges)))
