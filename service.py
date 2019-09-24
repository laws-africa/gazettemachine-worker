import tempfile
import os
import boto3
import botocore
import csv
import requests

API_AUTH_TOKEN = os.environ['API_AUTH_TOKEN']
API_URL = os.environ.get('API_URL', 'https://api.gazettes.laws.africa/v1')
TIMEOUT = 30
MIRROR_TARGETS = os.environ.get('MIRROR_TARGETS', '').split(' ')

session = requests.Session()
session.headers.update({'Authorization': 'Token %s' % API_AUTH_TOKEN})


def incoming_from_s3(event, context):
    """ S3 event.
    """
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')

        if key.lower().endswith('.pdf'):
            # dropbox/bw/foo.pdf
            # dropbox/bw/foo/bar.pdf
            parts = key.split("/")
            if len(parts) >= 3 and parts[0] == 'dropbox':
                info = {
                    's3_location': '/'.join([bucket, key]),
                    'jurisdiction': parts[1],
                }
                pdf_from_s3(info)
                return

        elif key.lower().endswith('.csv'):
            csv_from_s3(bucket, key)
            return

        print("Ignored: %s" % key)


def pdf_from_s3(info):
    print("Calling GM: %s" % info)
    resp = session.post(API_URL + '/gazettes/pending/', json=info, timeout=TIMEOUT)
    print("Result from GM %s: %s" % (resp.status_code, resp.text))
    resp.raise_for_status()


def csv_from_s3(bucket, key):
    """ CSV file, probably from scrapy.
    Each entry should have a 'jurisdiction' and 'url' column.
    """
    s3 = boto3.client('s3')

    with tempfile.TemporaryFile('w+b') as f:
        s3.download_fileobj(bucket, key, f)
        f.seek(0)
        text = f.read()

    reader = csv.DictReader(text.decode('utf-8').split("\n"))
    rows = list(reader)

    data = {'items': [{
        'jurisdiction': r['jurisdiction'],
        'source_url': r['url'],
    } for r in rows if r.get('jurisdiction') and r.get('url')]}

    print("Calling GM: %s" % data)
    resp = session.post(API_URL + '/gazettes/pending/bulk/', json=data, timeout=TIMEOUT)
    print("Result from GM %s: %s" % (resp.status_code, resp.text))
    resp.raise_for_status()

    s3.delete_object(Bucket=bucket, Key=key)


def archived_gazette_changed(event, context):
    """ S3 event. Archived gazette has been created or deleted.
    """
    s3 = boto3.client('s3')
    prefix = 'test/'

    for record in event['Records']:
        print("Got S3 event record: {}".format(record))

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')
        if not key.startswith(prefix):
            continue

        for access_key, secret_key, tgt_bucket, tgt_prefix in get_mirror_targets():
            s3_tgt = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            tgt_key = tgt_prefix + key[len(prefix):]

            print("Mirror from bucket: {}, key: {} to bucket: {}, key: {}".format(bucket, key, tgt_bucket, tgt_key))

            if record['eventName'].startswith('ObjectRemoved'):
                print("Deleting bucket: {}, key: {}".format(tgt_bucket, tgt_key))
                try:
                    s3_tgt.delete_object(Bucket=bucket, Key=key)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '403':
                        print("Ignoring: {}".format(e))
                    else:
                        raise e

            if record['eventName'].startswith('ObjectCreated'):
                print("Copying to bucket: {}, key: {}".format(tgt_bucket, tgt_key))
                with tempfile.TemporaryFile() as f:
                    s3.download_fileobj(bucket, key, f)
                    f.seek(0)
                    s3_tgt.upload_fileobj(f, tgt_bucket, tgt_key)


def get_mirror_targets():
    targets = []

    for target in MIRROR_TARGETS:
        # access-key:secret-key@bucket/prefix
        creds, loc = target.split('@')
        creds = creds.split(':')

        if '/' in loc:
            bucket, prefix = loc.split('/', 1)
        else:
            bucket = loc
            prefix = ''

        targets.append([creds[0], creds[1], bucket, prefix])

    return targets
