import sys
import os
import csv

FOS_ID_IDX = 0
FOS_NORM_NAME_IDX = 2
FOS_LEVEL_IDX = 5
FOS_PAPER_COUNT_IDX = 7


def read_mag_file(filepath):
    data = {}
    with open(filepath, 'r') as f:
        r = csv.reader(f, delimiter='\t')
        for row in r:
            rowid = int(row[0])
            if rowid not in data:
                data[rowid] = []
            data[rowid].append(row)
    return data


def children_fields(fosid, fos_children):
    return [int(row[1]) for row in fos_children[fosid]] if fosid in fos_children else []


def find_fos(norm_name, fos):
    for fosid in fos:
        for row in fos[fosid]:
            if row[FOS_NORM_NAME_IDX].lower().strip() == norm_name.lower().strip():
                return fosid


def create_tree(fosid, fos_children):
    remaining = [fosid]
    tree = set()
    while(len(remaining) > 0):
        fosid = remaining.pop(0)
        tree.add(fosid)
        for child in children_fields(fosid, fos_children):
            if child not in tree:
                remaining.append(child)
    return tree


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError('Invalid number of arguments. You need "python extract_fields.py MAG_DIR"')
    elif not os.path.exists(sys.argv[1]):
        raise ValueError('Does not exist: {}'.format(sys.argv[1]))
    elif not os.path.isdir(sys.argv[1]):
        raise ValueError('Not a directory: {}'.format(sys.argv[1]))
    
    fos = read_mag_file(os.path.join(sys.argv[1], 'FieldsOfStudy.txt'))
    fos_children = read_mag_file(os.path.join(sys.argv[1], 'FieldOfStudyChildren.txt'))
    
    cs = find_fos('computer science', fos)
    cs_fields = create_tree(cs, fos_children)

    totals = 0
    for fosid, fosdata in fos.items():
        for row in fosdata:
            if fosid in cs_fields:
                totals += int(row[FOS_PAPER_COUNT_IDX])
    print('Total: {}'.format(totals))
