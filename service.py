import json
import boto3

from gm.gm import GazetteMachine


def identify_and_archive(event, context):
    try:
        gm = GazetteMachine()
        info = gm.identify_and_archive(event)
    except ValueError as e:
        return {
            'status': 'error',
            'message': str(e),
        }
    return {
        'status': 'accepted',
        'info': info,
    }


def incoming_from_s3(event, context):
    ecs = boto3.client('ecs')

    for record in event['Records']:
        s3_location = '/'.join([record['s3']['bucket']['name'], record['s3']['object']['key']])
        info = {'s3_location': s3_location}

        ecs.run_task(
            cluster='default',
            taskDefinition='identify-gazette',
            overrides={
                'containerOverrides': [{
                    'command': ['--identify', '--info', json.dumps(info)],
                }]
            },
            startedBy='lambda',
            launchType='FARGATE',
        )

    return {
        'statusCode': 200,
    }
