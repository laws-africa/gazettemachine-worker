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
        # TODO: inject root and parent trace ids
        gm.ocr_and_update(args.info_path)
    else:
        parser.print_help()
