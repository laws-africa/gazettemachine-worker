import tempfile
import os
import csv
import re
import email

import boto3
import botocore
import requests

API_AUTH_TOKEN = os.environ['API_AUTH_TOKEN']
API_URL = os.environ.get('API_URL', 'https://api.gazettes.laws.africa/v1')
TIMEOUT = 30
MIRROR_TARGETS = [x.strip() for x in os.environ.get('MIRROR_TARGETS', '').split() if x.strip()]

session = requests.Session()
session.headers.update({'Authorization': 'Token %s' % API_AUTH_TOKEN})


def incoming_from_s3(event, context):
    """ S3 event.
    """
    for record in event['Records']:
        print(f"Got S3 event record: {record}")
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')

        if key.lower().endswith('.pdf'):
            # dropbox/bw/foo.pdf
            # dropbox/bw/foo/bar.pdf
            parts = key.split("/")
            if len(parts) >= 3 and parts[0] == 'dropbox':
                juri = parts[1].split(' ')[0].lower()
                info = {
                    's3_location': '/'.join([bucket, key]),
                    'jurisdiction': juri,
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
        'source_url': r['url'].replace(' ', '%20'),
    } for r in rows if r.get('jurisdiction') and r.get('url')]}

    print("Calling GM: %s" % data)
    resp = session.post(API_URL + '/gazettes/pending/bulk/', json=data, timeout=TIMEOUT)
    print("Result from GM %s: %s" % (resp.status_code, resp.text))
    resp.raise_for_status()

    s3.delete_object(Bucket=bucket, Key=key)


def email_from_s3(event, context):
    """ AWS SES put an encoming email into S3 for us to process.
    """
    for record in event['Records']:
        print(f"Got S3 event record: {record}")
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')

        # determine jurisdiction
        # email/za-fs/foo
        match = re.match(r'^email/([^/]+)/.+', key)
        if match:
            jurisdiction = match.group(1)

            # extract attachments from the email and add to the dropbox folder in s3
            s3 = boto3.client('s3')
            with tempfile.TemporaryFile() as f:
                s3.download_fileobj(bucket, key, f)
                f.seek(0)
                msg = email.message_from_string(f.read().decode('utf-8'))

            # clean up the message id to use a as a prefix to ensure the pdf names are unique
            msg_id = re.sub('[^a-zA-Z0-9@.-]', '-', msg['message-id'])

            for part in msg.walk():
                if part.get_content_type() == 'application/pdf':
                    print(f"Processing part: {part.items()}")

                    # ensure original attachment filename is usable
                    filename = part.get_filename().replace('/', '-')
                    if not filename.lower().endswith('.pdf'):
                        filename = filename + '.pdf'

                    with tempfile.TemporaryFile() as f:
                        tgt_key = f'dropbox/{jurisdiction}/{msg_id}/{filename}'
                        print(f"Writing attachment to {bucket}/{tgt_key}")
                        f.write(part.get_payload(decode=True))
                        f.seek(0)
                        s3.upload_fileobj(f, bucket, tgt_key)

            # delete object
            print(f"Deleting message from S3: {bucket}/{key}")
            s3.delete_object(Bucket=bucket, Key=key)
            return

        print(f"Ignored: {key}")


def archived_gazette_changed(event, context):
    """ S3 event. Archived gazette has been created or deleted.
    """
    s3 = boto3.client('s3')
    archive_prefix = 'archive/'

    for record in event['Records']:
        print("Got S3 event record: {}".format(record))

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')
        if not key.startswith(archive_prefix):
            continue

        for access_key, secret_key, endpoint, region, src_prefix, ops, tgt_bucket, tgt_prefix in get_mirror_targets():
            # this allows us to mirror only some countries
            if src_prefix and not key.startswith(src_prefix):
                continue

            s3_tgt = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  endpoint_url=endpoint, region_name=region)
            tgt_key = tgt_prefix + key[len(archive_prefix):]

            for operation in ops:
                if record['eventName'].startswith(operation):
                    print("Mirror from bucket: {}, key: {} to bucket: {}, key: {}".format(bucket, key, tgt_bucket,
                                                                                          tgt_key))

                    if record['eventName'].startswith('ObjectRemoved'):
                        print("Deleting object in bucket: {}, key: {}".format(tgt_bucket, tgt_key))
                        try:
                            s3_tgt.delete_object(Bucket=tgt_bucket, Key=tgt_key)
                        except botocore.exceptions.ClientError as e:
                            if e.response['Error']['Code'] == 'AccessDenied':
                                print("Ignoring: {}".format(e))
                            else:
                                print("Error: {}".format(e))
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
        # access-key:secret-key:endpoint:region@prefix:operations:bucket/prefix
        if not '@' in target:
            continue
        creds, loc = target.split('@')
        creds = creds.split(':')

        endpoint = creds[2]
        endpoint = f"https://{endpoint}" if endpoint else None

        src_prefix, ops, loc = loc.split(':', 2)
        ops = ops.split(',')

        if '/' in loc:
            bucket, tgt_prefix = loc.split('/', 1)
        else:
            bucket = loc
            tgt_prefix = ''

        targets.append([creds[0], creds[1], endpoint, creds[3] or None, src_prefix, ops, bucket, tgt_prefix])

    return targets
