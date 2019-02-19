import json
import boto3


def run_task(command):
    ecs = boto3.client('ecs')
    return ecs.run_task(
        cluster='default',
        taskDefinition='identify-gazette',
        overrides={
            'containerOverrides': [{'command': command}]
        },
        startedBy='lambda',
        launchType='FARGATE',
    )


def identify_and_archive(event, context):
    run_task(['--identify', '--info', json.dumps(event)])
    return {
        'status': 'processed',
    }


def incoming_from_s3(event, context):
    for record in event['Records']:
        s3_location = '/'.join([record['s3']['bucket']['name'], record['s3']['object']['key']])
        info = {'s3_location': s3_location}
        run_task(['--identify', '--info', json.dumps(info)])

    return {
        'status': 'processed',
    }
