import argparse
import json
import sys
import logging

from gm.gm import GazetteMachine

logging.basicConfig(stream=sys.stderr, level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gazette Machine')
    parser.add_argument('--identify', action='store_true', help="Identify and archive a gazette")
    parser.add_argument('--archive', action='store_true', help="Archive a gazette detailed in the info argument")
    parser.add_argument('--info', help='JSON-formatted info dictionary')

    args = parser.parse_args()
    gm = GazetteMachine()
    info = None

    if args.info:
        info = json.loads(args.info)

    if args.identify:
        if gm.identify_and_archive(info):
            sys.exit(0)
        else:
            sys.exit(1)

    if args.archive:
        if gm.archive(info):
            sys.exit(0)
        else:
            sys.exit(1)

    parser.print_help()
