import datetime
import codecs
import subprocess
import re


class GazetteMachine:
    NA_NUMBER_RE = re.compile(r'^No.\s+(\d+)$', re.MULTILINE)
    DATE_RE = re.compile(r'\b\d{1,2} (January|February|March|April|May|June|July|August|September|October|November|December) \d{4}\b')

    def get_coverpage_text(self, fname):
        result = subprocess.run(["pdftotext", "-f", "1", "-l", "1", fname, "outfile.txt"])
        result.check_returncode()

        with codecs.open("outfile.txt", "r", "utf-8") as f:
            return f.read()

    def identify_na(self, coverpage):
        identity = {}

        if not ('GOVERNMENT GAZETTE' in coverpage and 'REPUBLIC OF NAMIBIA' in coverpage):
            return identity

        identity['jurisdiction'] = 'na'

        # number
        match = self.NA_NUMBER_RE.search(coverpage)
        if match:
            identity['number'] = match.group(1)

        # date
        match = self.DATE_RE.search(coverpage)
        if match:
            date = datetime.datetime.strptime(match.group(), '%d %B %Y')
            identity['date'] = date.strftime('%Y-%m-%d')

        return identity


    def identify(self, fname):
        coverpage = self.get_coverpage_text(fname)
        identity = self.identify_na(coverpage)

        return identity


if __name__ == '__main__':
    gm = GazetteMachine()
    print(gm.identify('3564.pdf'))
