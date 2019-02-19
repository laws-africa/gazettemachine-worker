import datetime
import tempfile
import subprocess
import re
import logging
import os

import boto3
import requests


log = logging.getLogger(__name__)

GM_API_URL = os.environ.get('GM_API_URL', 'https://api.gazettes.laws.africa/v1')
GM_AUTH_TOKEN = os.environ.get('GM_AUTH_TOKEN')


class RequiresOCR(Exception):
    pass


class MetadataStore:
    """ Wrapper around the GazetteMachine metadata store API.
    """
    def __init__(self):
        self.base_url = GM_API_URL
        self.session = requests.Session()
        self.session.headers.update({'Authorization': 'Token %s' % GM_AUTH_TOKEN})

    def save_gazette(self, info):
        log.info("Saving gazette: %s" % info)
        resp = self.session.post(self.base_url + '/gazettes/', json=info)

        # already exists?
        if resp.status_code == 400 and "gazette with this key already exists." in resp.json().get('key', []):
            log.info("Gazette already exists")
            return False

        if resp.status_code == 400:
            log.info(resp.text)
        resp.raise_for_status()

        log.info("Responded %s" % resp.status_code)
        return resp.json()

    def manually_identify(self, info):
        log.info("Manualy identification: %s" % info)
        resp = self.session.post(self.base_url + '/tasks/', json={
            's3_location': info['s3_location'],
            'jurisdiction': info.get('jurisdiction'),
            'date': info.get('date'),
            'publication': info.get('publication'),
            'number': info.get('number'),
            'name': info.get('name'),
        })

        if resp.status_code == 400:
            log.info(resp.text)
        resp.raise_for_status()

        log.info("Responded %s" % resp.status_code)
        return resp.json()


class GazetteMachine:
    """ Magic for identifying and archiving gazettes.

    1. OCR if necessary
    2. Get coverpage text
    3. Attempt to identify.
    4. If succesful,
       4a. archive primary material into S3
       4b. archive source materials into S3 (if different to primary)
       4c. save info to database
       4d. trigger workflows
    5. If unsuccessful,
       5a. save info to database


    In general, +info+ is a dictionary with these keys

      jurisdiction: two letter country code (eg. "na") or jurisdiction such as ("za-gp")
      identified: successfully identified?
      s3_location: S3 details as one string, "bucket/key"
      sources: [s3_location, s3_location, ...]
      date: "YYYY-MM-DD"
      year: "YYYY"
      publication: name of publication, eg. Government Gazette
      key: fully unique key, eg. "na-government-gazette-dated-2018-01-01-no-31"
      frbr_work_uri: /na/gazette/2018-01-01/31
      name: friendly, formatted name, eg. "Namibia Government Gazette dated 2018-01-01 number 31"

    """

    INCOMING_BUCKET = 'lawsafrica-gazettes-incoming'
    DROPBOX_PATH = 'dropbox/'
    TEMP_PATH = 'temp/'
    ARCHIVE_BUCKET = 'lawsafrica-gazettes-archive'
    ARCHIVE_PATH = 'archive/'
    SOURCES_PATH = 'sources/'

    def __init__(self):
        self.s3 = boto3.client('s3')
        self.metadata = MetadataStore()

    def identify_and_archive(self, info):
        """ Attempt to identify and archive a gazette.

        The provided info may already be identified. If so, it will safely
        be archived.
        """
        prefix = "{}/{}".format(self.INCOMING_BUCKET, self.DROPBOX_PATH)

        if not info.get('s3_location', '').startswith(prefix):
            raise ValueError("Expected s3_location to start with '%s' but got '%s'" % (prefix, info.get('s3_location', '')))

        with self.fetch(info) as tmp:
            self.tmpfile = tmp

            if self.identify(info):
                log.info("Identified {} as {}".format(info['s3_location'], info['key']))
                return self.archive(info)
            else:
                log.info("Gazette {} requires manual identification".format(info['s3_location']))
                resp = self.manually_identify(info)
                info['manual_task_url'] = resp['url']
                return info

    def fetch(self, info):
        if 'fname' in info:
            return open(info['fname'], 'r+b')

        log.info("Dowloading {} from S3".format(info['s3_location']))

        tmp = tempfile.NamedTemporaryFile()
        bucket, key = info['s3_location'].split('/', 1)
        self.s3.download_fileobj(bucket, key, tmp)
        return tmp

    def identify(self, info):
        if not info.get('identified'):
            if not info.get('jurisdiction'):
                # lawsafrica-incoming/dropbox/na/file
                info['jurisdiction'] = info['s3_location'].split('/', 3)[2]

            if not info.get('jurisdiction'):
                return False

            # get identifier class
            identifier = globals()['Identifier%s' % info['jurisdiction'].upper()]()

            self.tmpfile.seek(0, 2)
            info['size'] = self.tmpfile.tell()
            self.tmpfile.seek(0)

            try:
                coverpage = self.get_coverpage_text()
            except RequiresOCR:
                self.ocr_to_s3(info)
                try:
                    coverpage = self.get_coverpage_text()
                except RequiresOCR:
                    return False

            identifier.identify(info, coverpage, self.tmpfile)

        if not info.get('identified'):
            return False

        #  fill in remaining details
        info['publication_code'] = info['publication'].lower().replace(' ', '-')
        info['key'] = '{jurisdiction}-{publication_code}-dated-{date}-no-{number}'.format(**info)
        info['name'] = '{jurisdiction_name} {publication} dated {date} number {number}'.format(**info)
        info['frbr_work_uri'] = '{jurisdiction}/{publication_code}/{date}/{number}'.format(**info)

        return True

    def manually_identify(self, info):
        """ Setup a task to manually identify this gazette.
        """
        self.metadata.manually_identify(info)

    def ocr_to_s3(self, info):
        """ OCR the file in f, write it back into f AND to S3, and update
        info to reflect the new file, moving the original into the sources list.
        """
        key = info['s3_location'].split('/', 1)[1]
        ocr_key = '{}-ocr.pdf'.format(key)
        ocr_location = '{}/{}/{}'.format(self.INCOMING_BUCKET, self.TEMP_PATH, ocr_key)

        # OCR the file in place
        self.ocr_file(self.tmpfile.name)

        self.tmpfile.flush()
        self.tmpfile.seek(0)

        log.info("Uploading OCRd {} to {}".format(info['s3_location'], ocr_location))

        # copy OCRd file to s3
        self.s3.put_object(Bucket=self.INCOMING_BUCKET, Key=ocr_key, Body=self.tmpfile)

        info.setdefault('sources', []).append(info['s3_location'])
        info['s3_location'] = ocr_location
        info['ocred'] = True

    def ocr_file(self, target):
        with tempfile.TemporaryDirectory() as tmpdir:
            # make a multipage tiff of the original PDF
            tiffs = "{}/images.tiff".format(tmpdir)
            result = subprocess.run(["gs", "-o", tiffs, "-sDEVICE=tiff32nc", "-dUseBigTIFF=true", "-r300", self.tmpfile.name])
            result.check_returncode()

            # OCR using tesseract to produce a pdf
            pdf = "{}/ocr-output".format(tmpdir)
            result = subprocess.run(["tesseract", tiffs, pdf, "pdf"])
            result.check_returncode()

            # convert images in resulting PDF to reduce size
            result = subprocess.run([
                "gs", "-dNOPAUSE", "-dBATCH", "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4", "-dPDFSETTINGS=/ebook", "-sOutputFile={}".format(target), "{}.pdf".format(pdf)])

    def archive(self, info):
        # final resting place
        dest = self.ARCHIVE_PATH + "{jurisdiction}/{year}/{key}.pdf".format(**info)

        # save to gazette metadata store
        temp = dict(info)
        temp['s3_location'] = "{}/{}".format(self.ARCHIVE_BUCKET, dest)
        saved = self.metadata.save_gazette(temp)

        # only archive if the gazette didn't already exist
        if saved is not False:
            # copy to archival S3 bucket
            log.info("Copying primary {} to {}/{}".format(info['s3_location'], self.ARCHIVE_BUCKET, dest))

            self.s3.copy_object(
                CopySource=info['s3_location'],
                Bucket=self.ARCHIVE_BUCKET,
                Key=dest,
            )

            # archive original sources, if they're different to the primary resource
            i = 0
            for source in info.get('sources', []):
                if source != info['s3_location']:
                    i += 1

                    # move to target S3 bucket
                    dest = self.SOURCES_PATH + info['key'] + "-source-{}.pdf".format(i)
                    log.info("Copying source {} to {}/{}".format(source, self.ARCHIVE_BUCKET, dest))

                    self.s3.copy_object(
                        CopySource=source,
                        Bucket=self.ARCHIVE_BUCKET,
                        Key=dest,
                    )

        self.cleanup(info)

        return saved

    def cleanup(self, info):
        for source in info.get('sources', []):
            if source != info['s3_location']:
                log.info("Deleting {}".format(source))
                bucket, key = source.split('/', 1)
                self.s3.delete_object(Bucket=bucket, Key=key)

        log.info("Deleting {}".format(info['s3_location']))
        bucket, key = info['s3_location'].split('/', 1)
        self.s3.delete_object(Bucket=bucket, Key=key)

    def get_coverpage_text(self):
        with tempfile.NamedTemporaryFile() as tmp:
            result = subprocess.run(["pdftotext", "-f", "1", "-l", "1", self.tmpfile.name, tmp.name])
            result.check_returncode()

            with open(tmp.name, "rt", encoding="utf-8") as f:
                coverpage = f.read()

        # is it decent?
        if len(coverpage.strip()) < 20 or 'gazette' not in coverpage.lower():
            raise RequiresOCR()

        return coverpage


class IdentifierNA:
    NUMBER_RE = re.compile(r'^No.\s+(\d+)$', re.MULTILINE)
    DATE_RE = re.compile(r'\b\d{1,2} (January|February|March|April|May|June|July|August|September|October|November|December) \d{4}\b')

    def identify(self, info, coverpage, f):
        info['identified'] = False

        if not ('GOVERNMENT GAZETTE' in coverpage and 'REPUBLIC OF NAMIBIA' in coverpage):
            return False

        info['jurisdiction_name'] = 'Namibia'
        info['publication'] = 'Government Gazette'

        # number
        match = self.NUMBER_RE.search(coverpage)
        if match:
            info['number'] = match.group(1)

        # date
        match = self.DATE_RE.search(coverpage)
        if match:
            date = datetime.datetime.strptime(match.group(), '%d %B %Y')
            info['date'] = date.strftime('%Y-%m-%d')
            info['year'] = str(date.year)

        info['identified'] = bool(info.get('number') and info.get('date'))


class IdentifierBW:
    NUMBER_RE = re.compile(r'\bNo.\s+(\d+)\b', re.MULTILINE)
    DATE_RE = re.compile(r'\b(\d{1,2})[a-z]{0,2} (January|February|March|April|May|June|July|August|September|October|November|December), (\d{4})\b')

    def identify(self, info, coverpage, f):
        info['identified'] = False

        if not ('GOVERNMENT GAZETTE' in coverpage and 'BOTSWANA' in coverpage):
            return False

        info['jurisdiction_name'] = 'Botswana'
        info['publication'] = 'Government Gazette'

        # number
        match = self.NUMBER_RE.search(coverpage)
        if match:
            info['number'] = match.group(1)

        # date
        match = self.DATE_RE.search(coverpage)
        if match:
            s = " ".join([match.group(1), match.group(2), match.group(3)])
            date = datetime.datetime.strptime(s, '%d %B %Y')
            info['date'] = date.strftime('%Y-%m-%d')
            info['year'] = str(date.year)

        info['identified'] = bool(info.get('number') and info.get('date'))
