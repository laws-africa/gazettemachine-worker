import json
import os
import boto3

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
        s3_location = '/'.join([record['s3']['bucket']['name'], record['s3']['object']['key'].replace('+', ' ')])
        info = {'s3_location': s3_location}
        run_task(['--identify', '--info', json.dumps(info)])

    return {
        'status': 'processed',
    }
