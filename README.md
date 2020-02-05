# Gazette Machine Worker

This is two things:

1. a Docker image that does heavy lifting for Gazette Machine, run as an ECS task.
2. a collection of AWS Lambdas that call out to the [Gazette Machine API](https://github.com/laws-africa/gazettemachine-api).

Image at Docker Hub: [lawsafrica/gazettemachine-worker](https://hub.docker.com/r/lawsafrica/gazettemachine-worker)

The `GazetteMachine-API-auth-token` auth token is stored in the AWS SSM Parameter Store. If this token is changed,
the AWS Lambda function must be redeployed (see Deployment section below) so that it picks up the new token.

## Local development

1. Clone this repo
2. Setup a python 3 virtual environment
3. Install dependencies: `pip3 install -r requirements.txt`
4. Run the worker: `python3 worker.py --help`

## Deployment

1. Install serverless and dependencies: `npm install`
2. Ensure you have AWS credentials setup in `~/.aws/credentials`
3. Deploy: `serverless deploy`

This will need to be done if the name of the S3 trigger function changes, or on very first deployment, to create the S3 trigger:

`serverless s3deploy`
