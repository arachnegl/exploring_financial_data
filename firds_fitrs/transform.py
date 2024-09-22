import datetime as dt
import io
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests
from constants import file_cache_root
from lxml import etree


class FcaXmlToDict:
    @classmethod
    def _ref_transform(cls, elem) -> dict[str, str | None]:
        namespaces = {
            "head003": "urn:iso:std:iso:20022:tech:xsd:head.003.001.01",
            "head001": "urn:iso:std:iso:20022:tech:xsd:head.001.001.01",
            "auth017": "urn:iso:std:iso:20022:tech:xsd:auth.017.001.02",
        }

        def findtext(elem, xpath: str) -> str | None:
            return elem.findtext(xpath, namespaces=namespaces)

        return {
            "Id": findtext(elem, ".//auth017:FinInstrmGnlAttrbts/auth017:Id"),
            "FullNm": findtext(elem, ".//auth017:FinInstrmGnlAttrbts/auth017:FullNm"),
            "ShrtNm": findtext(elem, ".//auth017:FinInstrmGnlAttrbts/auth017:ShrtNm"),
            "ClssfctnTp": findtext(
                elem, ".//auth017:FinInstrmGnlAttrbts/auth017:ClssfctnTp"
            ),
            "NtnlCcy": findtext(elem, ".//auth017:FinInstrmGnlAttrbts/auth017:NtnlCcy"),
            "CmmdtyDerivInd": findtext(
                elem, ".//auth017:FinInstrmGnlAttrbts/auth017:CmmdtyDerivInd"
            ),
            "Issr": findtext(elem, ".//auth017:Issr"),
            "TradgVnId": findtext(elem, ".//auth017:TradgVnRltdAttrbts/auth017:Id"),
            "IssrReq": findtext(elem, ".//auth017:TradgVnRltdAttrbts/auth017:IssrReq"),
            "AdmssnApprvlDtByIssr": findtext(
                elem, ".//auth017:TradgVnRltdAttrbts/auth017:AdmssnApprvlDtByIssr"
            ),
            "FrstTradDt": findtext(
                elem, ".//auth017:TradgVnRltdAttrbts/auth017:FrstTradDt"
            ),
            "TermntnDt": findtext(
                elem, ".//auth017:TradgVnRltdAttrbts/auth017:TermntnDt"
            ),
            "UnderlyingISIN": findtext(
                elem,
                ".//auth017:DerivInstrmAttrbts/auth017:UndrlygInstrm/auth017:Sngl/auth017:ISIN",
            ),
            "IncnsstncyInd": findtext(
                elem, ".//auth017:TechAttrbts/auth017:IncnsstncyInd"
            ),
            "RlvntCmptntAuthrty": findtext(
                elem, ".//auth017:TechAttrbts/auth017:RlvntCmptntAuthrty"
            ),
            "FrDt": findtext(
                elem, ".//auth017:TechAttrbts/auth017:PblctnPrd/auth017:FrDt"
            ),
            "RlvntTradgVn": findtext(
                elem, ".//auth017:TechAttrbts/auth017:RlvntTradgVn"
            ),
        }

    @classmethod
    def _eqty_trnsprncy_transform(cls, elem) -> dict[str, str | None]:
        namespaces = {
            "auth044": "urn:iso:std:iso:20022:tech:xsd:auth.044.001.02",
        }

        def findtext(elem, xpath: str) -> str | None:
            return elem.findtext(xpath, namespaces=namespaces)

        return {
            "TechRcrdId": findtext(elem, ".//auth044:TechRcrdId"),
            "Id": findtext(elem, ".//auth044:Id"),
            "FinInstrmClssfctn": findtext(elem, ".//auth044:FinInstrmClssfctn"),
            "FullNm": findtext(elem, ".//auth044:FullNm"),
            "Lqdty": findtext(elem, ".//auth044:Lqdty"),
            "Mthdlgy": findtext(elem, ".//auth044:Mthdlgy"),
            "LrgInScale": findtext(elem, ".//auth044:Sttstcs/auth044:LrgInScale"),
            "AvrgDalyNbOfTxs": findtext(
                elem, ".//auth044:Sttstcs/auth044:AvrgDalyNbOfTxs"
            ),
            "AvrgDalyTrnvr": findtext(elem, ".//auth044:Sttstcs/auth044:AvrgDalyTrnvr"),
            "RlvntMktId": findtext(elem, ".//auth044:RlvntMkt/auth044:Id"),
            "RptgPrd_FrDt": findtext(
                elem, ".//auth044:RptgPrd/auth044:FrDtToDt/auth044:FrDt"
            ),
            "RptgPrd_ToDt": findtext(
                elem, ".//auth044:RptgPrd/auth044:FrDtToDt/auth044:ToDt"
            ),
        }

    @classmethod
    def extract(cls, firds_or_fitrs: str, xml):
        # TODO try register_namespace
        firds_or_fitrs_to_tag_and_transform_cb = {
            "firds": (
                "{urn:iso:std:iso:20022:tech:xsd:auth.017.001.02}RefData",
                cls._ref_transform,
            ),
            "fitrs": (
                "{urn:iso:std:iso:20022:tech:xsd:auth.044.001.02}EqtyTrnsprncyData",
                cls._eqty_trnsprncy_transform,
            ),
        }
        tag, transform_cb = firds_or_fitrs_to_tag_and_transform_cb[firds_or_fitrs]

        # iterparse will parse the XML file in chunks
        # Could explore making this non-blocking using `XMLPullParser`
        for _, elem in etree.iterparse(io.BytesIO(xml), events=("end",), tag=tag):
            yield transform_cb(elem)
            elem.clear()


def download_xml(url: str) -> tuple[str, bytes]:
    res = requests.get(url)
    with ZipFile(io.BytesIO(res.content)) as zip_file:
        file_name = zip_file.namelist()[0]
        with zip_file.open(file_name) as file:
            return file_name, file.read()


def download_xml_to_pq(firds_or_fitrs: str, url: str) -> tuple[bool, Path]:
    file_name, xml = download_xml(url)
    xml_fname = Path(file_name)
    day_index = 2 if firds_or_fitrs == 'firds' else 1
    day = dt.datetime.strptime(xml_fname.stem.split("_")[day_index], "%Y%m%d").date()
    dst_dir = file_cache_root / firds_or_fitrs / f"day={day}"
    dst = dst_dir / (xml_fname.stem + ".pq")
    if dst.is_file():
        return False, dst
    else:
        dst_dir.mkdir(parents=True, exist_ok=True)
    refs = FcaXmlToDict.extract(firds_or_fitrs, xml)
    pd.DataFrame.from_records(refs).to_parquet(dst)
    return True, dst
