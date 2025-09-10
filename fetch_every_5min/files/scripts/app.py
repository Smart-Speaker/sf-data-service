#!/usr/bin/env python3
import json
import os
import csv
from datetime import datetime, timezone
from pathlib import Path
import sys

from simple_salesforce import (
    Salesforce,
    SalesforceMalformedRequest,
    SalesforceAuthenticationFailed,
)

# ---------- Constants / Paths ----------
BASE_DIR = Path(__file__).resolve().parent            # .../files/scripts
FILES_DIR = BASE_DIR.parent                           # .../files
OUT_DIR = FILES_DIR / "pricebook"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Config from ENV ----------
SF_USERNAME = os.environ.get("SF_USERNAME", "")
SF_PASSWORD = os.environ.get("SF_PASSWORD", "")
SF_SECURITY_TOKEN = os.environ.get("SF_SECURITY_TOKEN", "")
SF_DOMAIN = (os.environ.get("SF_DOMAIN") or "login").lower()

# Output filenames (placed in OUT_DIR)
OUTPUT_JSON_NAME = os.environ.get("OUTPUT_JSON_NAME", "pricebooks_export.json")
OUTPUT_CSV_NAME = os.environ.get("OUTPUT_CSV_NAME", "pricebooks_export.csv")

# Optional filter for entries
PRICEBOOK2_ID = (os.environ.get("PRICEBOOK2_ID") or "").strip() or None

# Include Product2 custom fields discovery?
INCLUDE_PRODUCT2_CUSTOM_FIELDS = (os.environ.get("INCLUDE_PRODUCT2_FIELDS", "true").lower() in ("1","true","yes","y"))

def header(title: str):
    print("\n" + "=" * 170)
    print(title)
    print("=" * 170)

def info(msg: str):
    print(f"- {msg}")

def require_env():
    missing = []
    for key, val in {
        "SF_USERNAME": SF_USERNAME,
        "SF_PASSWORD": SF_PASSWORD,
        "SF_SECURITY_TOKEN": SF_SECURITY_TOKEN,
    }.items():
        if not val:
            missing.append(key)
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

def discover_custom_fields(sf, sobject_name):
    desc = getattr(sf, sobject_name).describe()
    fields = []
    for f in desc.get("fields", []):
        name = f.get("name")
        if not name or not name.endswith("__c"):
            continue
        if f.get("deprecatedAndHidden"):
            continue
        if "queryable" in f and not f["queryable"]:
            continue
        fields.append(name)
    return fields

def build_all_pricebooks_soql():
    fields = [
        "Id","Name","IsActive","IsStandard","Description",
        "CreatedDate","LastModifiedDate",
    ]
    return f"SELECT {', '.join(fields)} FROM Pricebook2"

def build_flat_pbe_soql(include_currency_iso, pbe_custom_fields, product2_custom_fields, pricebook2_id=None):
    fields = [
        "Id","Pricebook2Id","Product2Id","UnitPrice","IsActive","UseStandardPrice","CreatedDate","LastModifiedDate",
        "Pricebook2.Id","Pricebook2.Name","Pricebook2.IsActive","Pricebook2.IsStandard","Pricebook2.Description",
        "Pricebook2.CreatedDate","Pricebook2.LastModifiedDate",
        "Product2.Name","Product2.ProductCode","Product2.Family","Product2.IsActive","Product2.Description",
    ]
    if include_currency_iso:
        fields.append("CurrencyIsoCode")
    if pbe_custom_fields:
        fields.extend(pbe_custom_fields)
    if product2_custom_fields:
        fields.extend([f"Product2.{f}" for f in product2_custom_fields])

    soql = f"SELECT {', '.join(fields)} FROM PricebookEntry"
    if pricebook2_id:
        soql += f" WHERE Pricebook2Id = '{pricebook2_id}'"
    return soql

def detect_multi_currency(sf) -> bool:
    try:
        sf.query("SELECT Id, CurrencyIsoCode FROM PricebookEntry LIMIT 1")
        return True
    except SalesforceMalformedRequest as e:
        msg = str(e)
        if "No such column 'CurrencyIsoCode' on entity 'PricebookEntry'" in msg:
            return False
        raise

def login_salesforce():
    header("LOGIN TO SALESFORCE (ENV)")
    info(f"Username : {SF_USERNAME}")
    info(f"Domain   : {SF_DOMAIN}")
    require_env()
    try:
        sf = Salesforce(
            username=SF_USERNAME,
            password=SF_PASSWORD,
            security_token=SF_SECURITY_TOKEN,
            domain=SF_DOMAIN,
        )
        info("Logged in (user+pass+token)")
        return sf
    except SalesforceAuthenticationFailed as e:
        details = getattr(e, "content", None) or str(e)
        raise RuntimeError(f"Salesforce login failed. Details: {details}") from e

def safe_rel(dct, rel_name, key, default=None):
    rel = dct.get(rel_name)
    if isinstance(rel, dict):
        return rel.get(key, default)
    return default

def main():
    sf = login_salesforce()

    header("DISCOVER METADATA")
    info("Discovering custom fields on PricebookEntry…")
    pbe_custom_fields = discover_custom_fields(sf, "PricebookEntry")
    info(f"PBE custom fields: {len(pbe_custom_fields)}")

    product2_custom_fields = []
    if INCLUDE_PRODUCT2_CUSTOM_FIELDS:
        info("Discovering custom fields on Product2…")
        product2_custom_fields = discover_custom_fields(sf, "Product2")
        info(f"Product2 custom fields: {len(product2_custom_fields)}")

    header("DETECT MULTI-CURRENCY")
    include_currency = detect_multi_currency(sf)
    info(f"Multi-currency available: {include_currency}")

    # Preload all pricebooks (even empty)
    header("QUERY PRICEBOOKS")
    pb_soql = build_all_pricebooks_soql()
    pricebooks_map = {}
    for pb in sf.query_all_iter(pb_soql):
        pricebooks_map[pb["Id"]] = {
            "Id": pb["Id"],
            "Name": pb.get("Name"),
            "IsActive": pb.get("IsActive"),
            "IsStandard": pb.get("IsStandard"),
            "Description": pb.get("Description"),
            "CreatedDate": pb.get("CreatedDate"),
            "LastModifiedDate": pb.get("LastModifiedDate"),
            "Entries": [],
        }
    info(f"Visible pricebooks fetched: {len(pricebooks_map)}")

    # Stream all entries
    pbe_soql = build_flat_pbe_soql(include_currency, pbe_custom_fields, product2_custom_fields, PRICEBOOK2_ID)
    header("QUERY PRICEBOOK ENTRIES (REST query_all_iter)")
    info("Streaming records…")

    base_cols = [
        "Pricebook.Id","Pricebook.Name","Entry.Id","Entry.Pricebook2Id","Entry.Product2Id",
        "Entry.UnitPrice","Entry.IsActive","Entry.UseStandardPrice","Entry.CreatedDate","Entry.LastModifiedDate",
        "Product.Id","Product.Name","Product.ProductCode","Product.Family","Product.IsActive","Product.Description",
    ]
    if include_currency:
        base_cols.insert(7, "Entry.CurrencyIsoCode")

    dynamic_cols = [f"Entry.{f}" for f in pbe_custom_fields]
    if INCLUDE_PRODUCT2_CUSTOM_FIELDS:
        dynamic_cols += [f"Product.{f}" for f in product2_custom_fields]
    header_cols = base_cols + dynamic_cols

    out_json = OUT_DIR / OUTPUT_JSON_NAME
    out_csv = OUT_DIR / OUTPUT_CSV_NAME

    total_entry_rows = 0
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as fcsv:
        writer = csv.DictWriter(
            fcsv,
            fieldnames=header_cols,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            escapechar="\\",
            lineterminator="\n",
        )
        writer.writeheader()

        try:
            for r in sf.query_all_iter(pbe_soql):
                total_entry_rows += 1

                pb_id = safe_rel(r, "Pricebook2", "Id") or r.get("Pricebook2Id")
                pb_name = safe_rel(r, "Pricebook2", "Name")

                if pb_id not in pricebooks_map:
                    pricebooks_map[pb_id] = {
                        "Id": pb_id,
                        "Name": pb_name,
                        "IsActive": safe_rel(r, "Pricebook2", "IsActive"),
                        "IsStandard": safe_rel(r, "Pricebook2", "IsStandard"),
                        "Description": safe_rel(r, "Pricebook2", "Description"),
                        "CreatedDate": safe_rel(r, "Pricebook2", "CreatedDate"),
                        "LastModifiedDate": safe_rel(r, "Pricebook2", "LastModifiedDate"),
                        "Entries": [],
                    }

                entry = {
                    "Id": r.get("Id"),
                    "Pricebook2Id": r.get("Pricebook2Id"),
                    "Product2Id": r.get("Product2Id"),
                    "UnitPrice": r.get("UnitPrice"),
                    "IsActive": r.get("IsActive"),
                    "UseStandardPrice": r.get("UseStandardPrice"),
                    "CreatedDate": r.get("CreatedDate"),
                    "LastModifiedDate": r.get("LastModifiedDate"),
                    "Product": {
                        "Id": r.get("Product2Id"),
                        "Name": safe_rel(r, "Product2", "Name"),
                        "ProductCode": safe_rel(r, "Product2", "ProductCode"),
                        "Family": safe_rel(r, "Product2", "Family"),
                        "IsActive": safe_rel(r, "Product2", "IsActive"),
                        "Description": safe_rel(r, "Product2", "Description"),
                    },
                }
                if include_currency:
                    entry["CurrencyIsoCode"] = r.get("CurrencyIsoCode")

                # Custom fields
                for fcf in pbe_custom_fields:
                    entry[fcf] = r.get(fcf)
                if INCLUDE_PRODUCT2_CUSTOM_FIELDS:
                    for fcf in product2_custom_fields:
                        entry["Product"][fcf] = safe_rel(r, "Product2", fcf)

                pricebooks_map[pb_id]["Entries"].append(entry)

                # CSV row
                row = {
                    "Pricebook.Id": pb_id,
                    "Pricebook.Name": pb_name,
                    "Entry.Id": entry["Id"],
                    "Entry.Pricebook2Id": entry["Pricebook2Id"],
                    "Entry.Product2Id": entry["Product2Id"],
                    "Entry.UnitPrice": entry["UnitPrice"],
                    "Entry.IsActive": entry["IsActive"],
                    "Entry.UseStandardPrice": entry["UseStandardPrice"],
                    "Entry.CreatedDate": entry["CreatedDate"],
                    "Entry.LastModifiedDate": entry["LastModifiedDate"],
                    "Product.Id": entry["Product"]["Id"],
                    "Product.Name": entry["Product"]["Name"],
                    "Product.ProductCode": entry["Product"]["ProductCode"],
                    "Product.Family": entry["Product"]["Family"],
                    "Product.IsActive": entry["Product"]["IsActive"],
                    "Product.Description": entry["Product"]["Description"],
                }
                if include_currency:
                    row["Entry.CurrencyIsoCode"] = entry.get("CurrencyIsoCode")

                for fcf in pbe_custom_fields:
                    row[f"Entry.{fcf}"] = entry.get(fcf)
                if INCLUDE_PRODUCT2_CUSTOM_FIELDS:
                    for fcf in product2_custom_fields:
                        row[f"Product.{fcf}"] = entry["Product"].get(fcf)

                writer.writerow(row)

        except SalesforceMalformedRequest as e:
            print("\n! REST query failed while streaming.")
            print(f"  {e}")
            raise

    # Extra TSV for convenience
    tsv_path = out_csv.with_suffix(".tsv")
    with open(out_csv, "r", encoding="utf-8-sig", newline="") as fin, \
         open(tsv_path, "w", encoding="utf-8-sig", newline="") as ftsv:
        reader = csv.reader(fin)
        tsv_writer = csv.writer(ftsv, delimiter="\t", lineterminator="\n")
        for row in reader:
            tsv_writer.writerow(row)

    info(f"Saved CSV : {os.path.abspath(os.fspath(out_csv))}")
    info(f"Saved TSV : {os.path.abspath(os.fspath(tsv_path))}")

    verified_lines = 0
    with open(out_csv, "r", encoding="utf-8-sig", newline="") as fcheck:
        for _ in csv.reader(fcheck):
            verified_lines += 1
    info(f"CSV physical lines (including header): {verified_lines}")

    # Sort and write JSON
    def _pb_sort_key(pb):
        return (0 if pb.get("IsStandard") else 1, (pb.get("Name") or "").lower())

    pricebooks = sorted(pricebooks_map.values(), key=_pb_sort_key)
    total_entries = sum(len(pb["Entries"]) for pb in pricebooks)

    output = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "pricebook_count": len(pricebooks),
        "total_entry_count": total_entries,
        "multi_currency": include_currency,
        "included_custom_fields": {
            "PricebookEntry": pbe_custom_fields,
            "Product2": product2_custom_fields
        },
        "pricebooks": pricebooks,
    }
    out_json.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    info(f"Saved JSON: {os.path.abspath(os.fspath(out_json))}")

    header("DONE")
    print(f"✅ Price books exported: {len(pricebooks)}")
    print(f"✅ Total entries exported: {total_entry_rows}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n" + "=" * 70)
        print("ERROR")
        print("=" * 70)
        print(f"{type(e).__name__}: {e}")
        sys.exit(1)
