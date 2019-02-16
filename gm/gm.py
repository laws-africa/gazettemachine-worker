import datetime
import tempfile
import subprocess
import re
import logging

import boto3


log = logging.getLogger(__name__)


class RequiresOCR(Exception):
    pass


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
      name: friendly, formatted name, eg. "Namibia Government Gazette dated 2018-01-01 number 3"

    """

    WORKING_BUCKET = 'lawsafrica-gazettes-working'
    ARCHIVE_BUCKET = 'lawsafrica-gazettes-archive'
    ARCHIVE_PATH = 'archive/'
    SOURCES_PATH = 'sources/'

    NA_NUMBER_RE = re.compile(r'^No.\s+(\d+)$', re.MULTILINE)
    DATE_RE = re.compile(r'\b\d{1,2} (January|February|March|April|May|June|July|August|September|October|November|December) \d{4}\b')

    def __init__(self):
        self.s3 = boto3.client('s3')

    def identify_and_archive(self, info):
        """ Attempt to identify and archive a gazette
        """
        with self.fetch(info) as tmp:
            self.tmpfile = tmp

            if self.identify(info):
                self.archive(info)
            else:
                self.manual_ident(info)

        return info

    def fetch(self, info):
        if 'fname' in info:
            return open(info['fname'], 'r+b')

        tmp = tempfile.NamedTemporaryFile()
        self.s3.download_file(info['s3_bucket'], info['s3_key'], tmp.name)
        return tmp

    def identify(self, info):
        try:
            coverpage = self.get_coverpage_text()
        except RequiresOCR:
            self.ocr_to_s3(info)
            coverpage = self.get_coverpage_text()

        identifier = {'na': self.identify_na}[info['jurisdiction']]
        identifier(info, coverpage)

        return info

    def ocr_to_s3(self, info):
        """ OCR the file in f, write it back into f AND to S3, and update
        info to reflect the new file, moving the original into the sources list.
        """
        key = info['s3_location'].split('/', 1)[1]
        ocr_key = '{}-ocr.pdf'.format(key)
        ocr_location = '{}/{}'.format(self.WORKING_BUCKET, ocr_key)

        # TODO: do OCR and write it into the original file
        with tempfile.NamedTemporaryFile() as tmp:
            self.ocr_file(tmp.name)

            # copy OCRd file into old one
            tmp.seek(0)
            self.tmpfile.seek(0)
            while True:
                data = tmp.read(4096)
                if not data:
                    break
                self.tmpfile.write(data)

        self.tmpfile.flush()
        self.tmpfile.seek(0)

        log.info("Uploading OCRd {} to {}".format(info['s3_location'], ocr_location))

        # copy OCRd file to s3
        self.s3.put_object(Bucket=self.WORKING_BUCKET, Key=ocr_key, Body=self.tmpfile)

        info.setdefault('sources', []).append(info['s3_location'])
        info['s3_location'] = ocr_location

    def ocr_file(self, fname):
        # TODO: ocr and optimise
        pass

    def archive(self, info):
        # move to target S3 bucket
        dest = self.ARCHIVE_PATH + info['key'] + ".pdf"
        log.info("Moving primary {} to {}/{}".format(info['s3_location'], self.ARCHIVE_BUCKET, dest))

        self.s3.copy_object(
            CopySource=info['s3_location'],
            Bucket=self.ARCHIVE_BUCKET,
            Key=dest,
        )
        bucket, key = info['s3_location'].split('/', 1)
        self.s3.delete_object(Bucket=bucket, Key=key)

        # move original sources, too, if they're different
        i = 0
        for source in info.get('sources', []):
            if source != info['s3_location']:
                i += 1

                # move to target S3 bucket
                dest = self.SOURCES_PATH + info['key'] + "-source-{}.pdf".format(i)
                log.info("Moving source {} to {}/{}".format(source, self.ARCHIVE_BUCKET, dest))

                self.s3.copy_object(
                    CopySource=source,
                    Bucket=self.ARCHIVE_BUCKET,
                    Key=dest,
                )
                bucket, key = source.split('/', 1)
                self.s3.delete_object(Bucket=bucket, Key=key)

        return info

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

    def identify_na(self, info, coverpage):
        info['identified'] = False

        if not ('GOVERNMENT GAZETTE' in coverpage and 'REPUBLIC OF NAMIBIA' in coverpage):
            return None

        info['jurisdiction'] = 'na'
        info['publication'] = 'government-gazette'

        # number
        match = self.NA_NUMBER_RE.search(coverpage)
        if match:
            info['number'] = match.group(1)

        # date
        match = self.DATE_RE.search(coverpage)
        if match:
            date = datetime.datetime.strptime(match.group(), '%d %B %Y')
            info['date'] = date.strftime('%Y-%m-%d')
            info['year'] = str(date.year)

        info['identified'] = bool(info.get('number') and info.get('date'))
        if info['identified']:
            info['id'] = '{jurisdiction}-{publication}-dated-{date}-no-{number}'.format(**info)
            info['key'] = '{jurisdiction}/{year}/{id}.pdf'.format(**info)
            info['name'] = 'Namibia Government Gazette dated {date} number {number}'.format(**info)
            info['frbr_work_uri'] = '{jurisdiction}/gazette/{date}/{number}'.format(**info)

        return info['identified']
