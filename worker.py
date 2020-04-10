import argparse
import sys
import logging

from aws_xray_sdk.core import xray_recorder, patch

from gm.worker import Worker

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

xray_recorder.configure(
    service='GazetteMachineWorker',
    sampling=False,
    plugins=('ECSPlugin',),
)
patch(['requests'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gazette Machine')
    parser.add_argument('--ocr', action='store_true', help="Identify and archive a gazette")
    parser.add_argument('--info-path', help='Path fragment for URL to fetch/update info')

    args = parser.parse_args()
    gm = Worker()
    info = None

    if args.ocr:
        # TODO: inject root and parent trace ids
        with xray_recorder.in_segment():
            with xray_recorder.in_subsegment('ocr'):
                gm.ocr_and_update(args.info_path)
    else:
        parser.print_help()
