import datetime
import tempfile
import codecs
import subprocess
import re

import boto3
import uuid

s3_client = boto3.client('s3')


NA_NUMBER_RE = re.compile(r'^No.\s+(\d+)$', re.MULTILINE)
DATE_RE = re.compile(r'\b\d{1,2} (January|February|March|April|May|June|July|August|September|October|November|December) \d{4}\b')

def get_coverpage_text(fname):
    with tempfile.NamedTemporaryFile() as tmp:
        result = subprocess.run(["pdftotext", "-f", "1", "-l", "1", fname, tmp.name])
        result.check_returncode()

        with codecs.open(tmp.name, "r", "utf-8") as f:
            return f.read()


def identify_na(coverpage):
    identity = {}

    if not ('GOVERNMENT GAZETTE' in coverpage and 'REPUBLIC OF NAMIBIA' in coverpage):
        return identity

    identity['jurisdiction'] = 'na'

    # number
    match = NA_NUMBER_RE.search(coverpage)
    if match:
        identity['number'] = match.group(1)

    # date
    match = DATE_RE.search(coverpage)
    if match:
        date = datetime.datetime.strptime(match.group(), '%d %B %Y')
        identity['date'] = date.strftime('%Y-%m-%d')

    return identity


def identify(event, context):
    if 'fname' in event:
        coverpage = get_coverpage_text(event['fname'])
    else:
        with tempfile.NamedTemporaryFile() as tmp:
            s3_client.download_file(event['s3_bucket'], event['s3_key'], tmp.name)
            coverpage = get_coverpage_text(tmp.name)

    event['identity'] = identify_na(coverpage)

    return event


if __name__ == '__main__':
    print(identify({'fname': '3564.pdf'}, {}))
