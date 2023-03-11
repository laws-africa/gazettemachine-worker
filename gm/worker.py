import tempfile
import subprocess
import logging
import os

import boto3
import requests


log = logging.getLogger(__name__)

GM_API_URL = os.environ.get('GM_API_URL', 'https://api.gazettes.laws.africa')
GM_AUTH_TOKEN = os.environ.get('GM_AUTH_TOKEN')

TIMEOUT = 30


class Worker:
    """ Worker process for OCRing gazettes and updating GM
    with the resulting info
    """
    INCOMING_BUCKET = 'lawsafrica-gazettes-incoming'
    TEMP_PATH = 'temp/'

    def __init__(self):
        self.s3 = boto3.client('s3')
        self.headers = {'Authorization': 'Token %s' % GM_AUTH_TOKEN}

    def ocr_and_update(self, path):
        url = GM_API_URL + path

        info = self.info_from_gm(url)
        self.fetch(info["s3_location"])
        info["s3_location"] = self.ocr_to_s3(info["s3_location"])
        self.update_gm(info, url)

    def info_from_gm(self, url):
        log.info(f"Fetching info from {url}")
        resp = requests.get(url, timeout=TIMEOUT, headers=self.headers)
        resp.raise_for_status()

        info = resp.json()
        log.info(f"Received: {info}")
        return info

    def fetch(self, s3_location):
        log.info(f"Dowloading {s3_location} from S3")

        tmp = tempfile.NamedTemporaryFile()
        bucket, key = s3_location.split('/', 1)
        self.s3.download_fileobj(bucket, key, tmp)
        tmp.flush()
        tmp.seek(0)

        self.tmpfile = tmp

    def ocr_to_s3(self, s3_location):
        """ OCR the file in f, write it back into f AND to S3, returning the new s3 location. """
        # OCR the file in place
        self.ocr_file(self.tmpfile.name)
        self.tmpfile.flush()

        self.tmpfile.seek(0, 2)
        self.tmpfile.seek(0)

        key = s3_location.split('/', 1)[1]
        ocr_key = f'{self.TEMP_PATH}{key}-ocr.pdf'
        ocr_location = f'{self.INCOMING_BUCKET}/{ocr_key}'

        log.info(f"Uploading OCRd file to {ocr_location}")
        self.s3.put_object(Bucket=self.INCOMING_BUCKET, Key=ocr_key, Body=self.tmpfile)
        return ocr_location

    def ocr_file(self, target):
        with tempfile.TemporaryDirectory() as tmpdir:
            # make a multipage tiff of the original PDF
            tiffs = f"{tmpdir}/images.tiff"
            result = subprocess.run(["gs", "-o", tiffs, "-sDEVICE=tiff32nc", "-dUseBigTIFF=true", "-r300", self.tmpfile.name])
            result.check_returncode()

            # OCR using tesseract to produce a pdf
            pdf = f"{tmpdir}/ocr-output"
            result = subprocess.run(["tesseract", tiffs, pdf, "pdf"])
            result.check_returncode()

            # convert images in resulting PDF to reduce size
            result = subprocess.run([
                "gs", "-dNOPAUSE", "-dBATCH", "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4", "-dPDFSETTINGS=/ebook",
                f"-sOutputFile={target}", f"{pdf}.pdf"])
            result.check_returncode()

    def update_gm(self, info, url):
        log.info(f"Updating GM {url}: {info}")
        resp = requests.put(url, timeout=TIMEOUT, json=info, headers=self.headers)
        log.info(f"Response: {resp.text}")
        resp.raise_for_status()

        info = resp.json()
        log.info(f"Received: {info}")
