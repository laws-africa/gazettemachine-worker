# Gazette Machine Worker

[![Build Status](https://travis-ci.org/laws-africa/gazettemachine-worker.svg)](http://travis-ci.org/laws-africa/gazettemachine-worker)

This is two things:

1. a Docker image that does heavy lifting for Gazette Machine
2. a collection of AWS Lambdas that kick off the Gazette Machine via ECS.

It calls the [Gazette Machine Storage API](https://github.com/laws-africa/gazettemachine) to store metadata.

Image at Docker Hub: [lawsafrica/gazettemachine-worker](https://hub.docker.com/r/lawsafrica/gazettemachine-worker)

The `GazetteMachine-API-auth-token` auth token is stored in the AWS SSM Parameter Store.

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
