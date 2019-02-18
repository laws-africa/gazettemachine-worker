# Gazette Machine Worker

This is a Docker image that does heavy lifting for Gazette Machine.

It calls the [Gazette Machine Storage API](https://github.com/laws-africa/gazettemachine) to store metadata.

## Local development

1. Clone this repo
2. Setup a python 3 virtual environment
3. Install dependencies: `pip3 install -r requirements.txt`
4. Run the worker: `python3 worker.py --help`
