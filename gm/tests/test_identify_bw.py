import unittest

from gm.gm import IdentifierBW


class IdentifyBWTest(unittest.TestCase):
    def setUp(self):
        self.ident = IdentifierBW()

    def test_base(self):
        info = {}
        self.ident.identify(info, """
REPUBLIC OF BOTSWANA

GOVERNMENT GAZETTE
EXTRAORDINARY
Vol. LV, No. 17

GABORONE

_

23rd March, 2018

CONTENTS
Page

Notice of Revision of Public Transport Passenger Fares — G.N. No. 182 Of 2018.......s:cssccssesecsscesessstesseneetenseees2646
The following Supplementis published with this issue of the Gazette —
Supplement B — Extradition (Amendment) Bill, 2018 — Bill No. 9 Of 2018.0...ssecesseesneeesssesetsnseenees B57 -58

The Botswana Government Gazetteis printed by Department of GovernmentPrinting and Publishing Services,

Private Bag 0081, GABORONE, Republic of Botswana. Annual subscription rates are P600,00 post mail, SADC
Countries airmail P1,357,00, Rest of Africa airmail P1,357,00, Europe and USAairmail P1,735,00.
The price for this issue of the Extraodinary Gazette (inclusive of supplement) is P1.00
""", None)
        self.assertEqual({
            'identified': True,
            'jurisdiction_name': 'Botswana',
            'publication': 'Government Gazette',
            'date': '2018-03-23',
            'number': '17',
            'year': '2018',
        }, info)
