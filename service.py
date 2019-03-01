import tempfile
import os
import boto3
import csv
import requests

API_AUTH_TOKEN = os.environ['API_AUTH_TOKEN']
API_URL = os.environ.get('API_URL', 'https://api.gazettes.laws.africa/v1')
TIMEOUT = 30

session = requests.Session()
session.headers.update({'Authorization': 'Token %s' % API_AUTH_TOKEN})


def incoming_from_s3(event, context):
    """ S3 event.
    """
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')

        if key.endswith('.pdf'):
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

        elif key.endswith('.csv'):
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

    with tempfile.TemporaryFile() as f:
        s3.download_fileobj(bucket, key, f)
        f.seek(0)
        reader = csv.DictReader(f)

        # filter out the URLs we've already processed
        rows = list(reader)
        urls = [r['url'] for r in rows if r.get('url') and r.get('jurisdiction')]

        resp = session.post(API_URL + '/filter-urls', json={'urls': urls}, timeout=TIMEOUT)
        resp.raise_for_status()
        print("Responded %s" % resp.status_code)
        urls = set(resp.json()['urls'])
        print("URLs to process: %s" % urls)

        for row in rows:
            if row['url'] in urls:
                info = {
                    'jurisdiction': row['jurisdiction'],
                    'source_url': row['url'],
                }
                print("Calling GM: %s" % info)
                resp = session.post(API_URL + '/gazettes/pending/', json=info, timeout=TIMEOUT)
                print("Result from GM %s: %s" % (resp.status_code, resp.text))
                resp.raise_for_status()
