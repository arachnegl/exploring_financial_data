import datetime as dt
from pathlib import Path

import polars as pl
from lxml import etree


def transform_lei_record(elem) -> dict:
    namespaces = {"lei": "http://www.gleif.org/data/schema/leidata/2016"}

    def findtext(elem, xpath: str) -> str | None:
        return elem.findtext(xpath, namespaces=namespaces)

    return {
        "LEI": findtext(elem, ".//lei:LEI"),
        "LegalName": findtext(elem, ".//lei:Entity/lei:LegalName"),
        "LegalAddress": {
            "FirstAddressLine": findtext(
                elem, ".//lei:Entity/lei:LegalAddress/lei:FirstAddressLine"
            ),
            "AdditionalAddressLine": findtext(
                elem, ".//lei:Entity/lei:LegalAddress/lei:AdditionalAddressLine"
            ),
            "City": findtext(elem, ".//lei:Entity/lei:LegalAddress/lei:City"),
            "Region": findtext(elem, ".//lei:Entity/lei:LegalAddress/lei:Region"),
            "Country": findtext(elem, ".//lei:Entity/lei:LegalAddress/lei:Country"),
            "PostalCode": findtext(
                elem, ".//lei:Entity/lei:LegalAddress/lei:PostalCode"
            ),
        },
        "EntityStatus": findtext(elem, ".//lei:Entity/lei:EntityStatus"),
        "EntityCreationDate": findtext(elem, ".//lei:Entity/lei:EntityCreationDate"),
        "Registration": {
            "InitialRegistrationDate": findtext(
                elem, ".//lei:Registration/lei:InitialRegistrationDate"
            ),
            "LastUpdateDate": findtext(elem, ".//lei:Registration/lei:LastUpdateDate"),
            "NextRenewalDate": findtext(
                elem, ".//lei:Registration/lei:NextRenewalDate"
            ),
            "RegistrationStatus": findtext(
                elem, ".//lei:Registration/lei:RegistrationStatus"
            ),
        },
    }


def load(fname, pdf: pl.DataFrame) -> Path:
    pdf.write_parquet(fname)
    return fname


def extract(file_path):

    content_date = None
    record_count = None

    at = 0

    for _, elem in etree.iterparse(file_path, events=("end",), tag="*"):
        if elem.tag == "{http://www.gleif.org/data/schema/leidata/2016}ContentDate":
            content_date = dt.datetime.strptime(elem.text, "%Y-%m-%dT%H:%M:%SZ").date()
            print(f"content_date: {content_date}")
        if elem.tag == "{http://www.gleif.org/data/schema/leidata/2016}RecordCount":
            record_count = int(elem.text)
            print(f"expected number: {record_count}")
        if elem.tag == "{http://www.gleif.org/data/schema/leidata/2016}LEIRecord":
            yield transform_lei_record(elem)
            # Clear the element from memory to save resources
            elem.clear()

            at += 1
            if at % 500_000 == 0:
                print(f"at: {at}")

    if record_count != at:
        print(f"prob: {at} expected {record_count}")

    print(f"done: {content_date} with: {at}")


def etl(fname):
    src = Path(fname)
    lei_records = extract(fname)
    pdf = pl.DataFrame(lei_records).unnest("LegalAddress").unnest("Registration")
    out = load(src.stem + ".pq", pdf)
    print(f"{out}")
