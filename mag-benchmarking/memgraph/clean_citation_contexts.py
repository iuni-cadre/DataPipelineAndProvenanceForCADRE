import os
import argparse
import unicodedata


def parse_cmd_args():
    parser = argparse.ArgumentParser(
        description=(''),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('src', type=str, help='')
    parser.add_argument('dest', type=str, help='')

    args = parser.parse_args()

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

    return args.src, args.dest


if __name__ == '__main__':
    src, dest = parse_cmd_args()
    with open(src, 'rb') as srcf:
        with open(dest, 'wb') as destf:
            count = 0
            for line in srcf:
                count += 1

                # remove UTF-8 control characters
                new_line = ''.join(filter(lambda char: unicodedata.category(char) != 'Cc', line.decode('utf-8'))).encode()

                # remove null bytes
                while b'\0' in new_line:
                    print('Found null byte on line {}'.format(count))
                    new_line = new_line.replace(b'\0', b'')

                destf.write(new_line)
                if not new_line.endswith(b'\n'):
                    destf.write(b'\n')
