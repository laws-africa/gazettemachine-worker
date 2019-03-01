import argparse
import sys
import logging

from gm.worker import Worker

logging.basicConfig(stream=sys.stderr, level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gazette Machine')
    parser.add_argument('--ocr', action='store_true', help="Identify and archive a gazette")
    parser.add_argument('--info-path', help='Path fragment for URL to fetch/update info')

    args = parser.parse_args()
    gm = Worker()
    info = None

    if args.ocr:
        if gm.ocr_and_update(args.info_path):
            sys.exit(0)
        else:
            sys.exit(1)

    parser.print_help()
