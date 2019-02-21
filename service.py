import tempfile
import json
import os
import boto3
import csv
import gm

API_AUTH_TOKEN = os.environ['API_AUTH_TOKEN']


def run_task(command):
    ecs = boto3.client('ecs')
    return ecs.run_task(
        cluster='default',
        taskDefinition='identify-gazette',
        startedBy='lambda',
        launchType='FARGATE',
        overrides={
            'containerOverrides': [{
                'name': 'GazetteMachineWorker',
                'command': command,
            }],
            'taskRoleArn': 'arn:aws:iam::254881051502:role/GazetteMachineTaskRole',
        },
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': ['subnet-0021aa9617f6ddcac', 'subnet-0f5d0adbb8cfd8baf'],
                'assignPublicIp': 'ENABLED',
            }
        },
    )


def identify_and_archive(event, context):
    """ API Gateway call.
    """
    # check authentication
    token = event.pop('auth-token', None)
    if token != API_AUTH_TOKEN:
        return {'statusCode': 403}

    run_task(['--identify', '--info', json.dumps(event)])

    return {'statusCode': 200}


def incoming_from_s3(event, context):
    """ S3 event.
    """
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'].replace('+', ' ')

        if key.endswith('.pdf'):
            pdf_from_s3({'s3_location': '/'.join([bucket, key])})

        elif key.endswith('.csv'):
            csv_from_s3(bucket, key)

    return {
        'status': 'processed',
    }


def pdf_from_s3(info):
    run_task(['--identify', '--info', json.dumps(info)])


def csv_from_s3(bucket, key):
    """ CSV file, probably from scrapy.
    Each entry should have a 'jurisdiction' and 'url' column.
    """
    s3 = boto3.client('s3')
    metadata = gm.MetadataStore()

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
