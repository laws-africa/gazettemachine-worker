import tempfile
import json
import os
import boto3
import csv
import requests

API_AUTH_TOKEN = os.environ['API_AUTH_TOKEN']
API_URL = os.environ.get('API_URL', 'https://api.gazettes.laws.africa/v1')


def incoming_from_s3(event, context):
    """ S3 event.
    """
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')

        if key.endswith('.pdf'):
            info = {
                's3_location': '/'.join([bucket, key]),
                'jurisdiction': key.split('/')[0],
            }
            pdf_from_s3(info)

        elif key.endswith('.csv'):
            csv_from_s3(bucket, key)

        else:
            print("Ignored: %s" % key)


def pdf_from_s3(info):
    headers = {'Authorization': 'Token %s' % API_AUTH_TOKEN}
    resp = requests.put(API_URL + '/gazettes/pending', json=info, headers=headers)
    print("Result from GM: %s" % resp.text)
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
        urls = set(metadata.filter_urls(urls))

        for row in rows:
            if row['url'] in urls:
                info = {
                    'jurisdiction': row['jurisdiction'],
                    'source_url': row['url'],
                }
                run_task(['--identify', '--info', json.dumps(info)])
