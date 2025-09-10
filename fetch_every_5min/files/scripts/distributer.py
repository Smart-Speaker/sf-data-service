import os
import json
import csv
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent  # .../files

# ENV overrides
PRICEBOOK_DIR = Path(os.environ.get("PRICEBOOK_DIR", str(BASE_DIR / "pricebook")))
INPUT_JSON = Path(os.environ.get("PRICEBOOK_JSON", str(PRICEBOOK_DIR / "pricebooks_export.json")))
OUTPUT_DIR = Path(os.environ.get("DISTRIBUTER_OUTPUT_DIR", str(BASE_DIR / "salesforce")))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_ENTRIES = OUTPUT_DIR / "pricebookEntries.csv"
OUT_PRICEBOOKS = OUTPUT_DIR / "pricebooks.csv"
OUT_PRODUCTS = OUTPUT_DIR / "products.csv"

FIXED_USER_ID = os.environ.get("DISTRIBUTER_FIXED_USER_ID", "005N1000006UI0rIAG")
FALSE_STR = "FALSE"

ENTRY_HEADERS = [
    "CreatedById","CreatedDate","Id","IsActive","IsArchived","IsDeleted",
    "LastModifiedById","LastModifiedDate","Mark_Up__c","Name","Onemedia_discount__c",
    "Onemedia_unit_cost__c","Pricebook2Id","Product2Id","ProductCode","SystemModstamp",
    "Trade_Unit_Price__c","Trade_discount__c","Tripleplay_Unit_Price__c","Tripleplay_discount__c",
    "UnitPrice","UseStandardPrice","X1_years_apps_discount__c",
]

PRICEBOOK_HEADERS = [
    "CreatedById","CreatedDate","Description","Id","IsActive","IsArchived","IsDeleted",
    "IsStandard","LastModifiedById","LastModifiedDate","LastReferencedDate","LastViewedDate",
    "Name","SystemModstamp",
]

PRODUCT_HEADERS = [
    "Automated__c","CASESAFE__c","Contract_Renewal__c","CreatedById","CreatedDate","DP_ASO__c",
    "DP_Ext_War__c","DP_Prem_1Yr__c","DP_Prem_3Yr__c","DP_Prem_5Yr_Plus__c","DP_Prem_5Yr__c",
    "Description","DisplayUrl","ExternalDataSourceId","External_Key__c","Family","Id","IsActive",
    "IsArchived","IsDeleted","LastModifiedById","LastModifiedDate","LastReferencedDate",
    "LastViewedDate","MDS_304_3S_1YR__c","MDS_304_3S_3YR__c","MDS_304_3S_5YR__c",
    "MDS_304_FSC_1YR__c","MDS_304_FSC_3YR__c","MDS_304_FSC_5YR__c","Manufacturer__c",
    "Manufacturer_search__c","Mark_Up__c","Name","OL_Support_Premium__c","OL_Suppt_Stan__c",
    "P_SUPPT_STAN__c","ProductCode","Product_Category__c","QuantityUnitOfMeasure",
    "Quantity_is_term__c","StockKeepingUnit","Support__c","SystemModstamp","Term__c",
    "WMS_Support__c","X1YR_OM_SO_WARRANTY__c","X2YR_OM_SO_WARRANTY__c","X3YR_OM_SO_WARRANTY__c",
]

def get(d, *keys, default=""):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur if cur is not None else default

def entry_to_row(entry, now_iso):
    product = entry.get("Product", {}) or {}
    name_val = get(entry, "Name") or get(product, "Name")
    row = {
        "CreatedById": FIXED_USER_ID,
        "LastModifiedById": FIXED_USER_ID,
        "IsArchived": FALSE_STR,
        "IsDeleted": FALSE_STR,
        "SystemModstamp": now_iso,
        "CreatedDate": get(entry, "CreatedDate"),
        "Id": get(entry, "Id"),
        "IsActive": get(entry, "IsActive"),
        "LastModifiedDate": get(entry, "LastModifiedDate"),
        "Mark_Up__c": get(entry, "Mark_Up__c"),
        "Name": name_val,
        "Onemedia_discount__c": get(entry, "Onemedia_discount__c"),
        "Onemedia_unit_cost__c": get(entry, "Onemedia_unit_cost__c"),
        "Pricebook2Id": get(entry, "Pricebook2Id"),
        "Product2Id": get(entry, "Product2Id"),
        "ProductCode": get(product, "ProductCode"),
        "Trade_Unit_Price__c": get(entry, "Trade_Unit_Price__c"),
        "Trade_discount__c": get(entry, "Trade_discount__c"),
        "Tripleplay_Unit_Price__c": get(entry, "Tripleplay_Unit_Price__c"),
        "Tripleplay_discount__c": get(entry, "Tripleplay_discount__c"),
        "UnitPrice": get(entry, "UnitPrice"),
        "UseStandardPrice": get(entry, "UseStandardPrice"),
        "X1_years_apps_discount__c": get(entry, "X1_years_apps_discount__c"),
    }
    for h in ENTRY_HEADERS:
        row.setdefault(h, "")
    return row

def pricebook_to_row(pb, now_iso):
    row = {
        "CreatedById": FIXED_USER_ID,
        "LastModifiedById": FIXED_USER_ID,
        "IsArchived": FALSE_STR,
        "IsDeleted": FALSE_STR,
        "SystemModstamp": now_iso,
        "CreatedDate": get(pb, "CreatedDate"),
        "Description": get(pb, "Description"),
        "Id": get(pb, "Id"),
        "IsActive": get(pb, "IsActive"),
        "IsStandard": get(pb, "IsStandard"),
        "LastModifiedDate": get(pb, "LastModifiedDate"),
        "LastReferencedDate": get(pb, "LastReferencedDate"),
        "LastViewedDate": get(pb, "LastViewedDate"),
        "Name": get(pb, "Name"),
    }
    for h in PRICEBOOK_HEADERS:
        row.setdefault(h, "")
    return row

def product_to_row(prod, now_iso):
    row = {
        "CreatedById": FIXED_USER_ID,
        "LastModifiedById": FIXED_USER_ID,
        "IsArchived": FALSE_STR,
        "IsDeleted": FALSE_STR,
        "SystemModstamp": now_iso,
        "Automated__c": get(prod, "Automated__c"),
        "CASESAFE__c": get(prod, "CASESAFE__c"),
        "Contract_Renewal__c": get(prod, "Contract_Renewal__c"),
        "CreatedDate": get(prod, "CreatedDate"),
        "DP_ASO__c": get(prod, "DP_ASO__c"),
        "DP_Ext_War__c": get(prod, "DP_Ext_War__c"),
        "DP_Prem_1Yr__c": get(prod, "DP_Prem_1Yr__c"),
        "DP_Prem_3Yr__c": get(prod, "DP_Prem_3Yr__c"),
        "DP_Prem_5Yr_Plus__c": get(prod, "DP_Prem_5Yr_Plus__c"),
        "DP_Prem_5Yr__c": get(prod, "DP_Prem_5Yr__c"),
        "Description": get(prod, "Description"),
        "DisplayUrl": get(prod, "DisplayUrl"),
        "ExternalDataSourceId": get(prod, "ExternalDataSourceId"),
        "External_Key__c": get(prod, "External_Key__c"),
        "Family": get(prod, "Family"),
        "Id": get(prod, "Id"),
        "IsActive": get(prod, "IsActive"),
        "LastModifiedDate": get(prod, "LastModifiedDate"),
        "LastReferencedDate": get(prod, "LastReferencedDate"),
        "LastViewedDate": get(prod, "LastViewedDate"),
        "MDS_304_3S_1YR__c": get(prod, "MDS_304_3S_1YR__c"),
        "MDS_304_3S_3YR__c": get(prod, "MDS_304_3S_3YR__c"),
        "MDS_304_3S_5YR__c": get(prod, "MDS_304_3S_5YR__c"),
        "MDS_304_FSC_1YR__c": get(prod, "MDS_304_FSC_1YR__c"),
        "MDS_304_FSC_3YR__c": get(prod, "MDS_304_FSC_3YR__c"),
        "MDS_304_FSC_5YR__c": get(prod, "MDS_304_FSC_5YR__c"),
        "Manufacturer__c": get(prod, "Manufacturer__c"),
        "Manufacturer_search__c": get(prod, "Manufacturer_search__c"),
        "Mark_Up__c": get(prod, "Mark_Up__c"),
        "Name": get(prod, "Name"),
        "OL_Support_Premium__c": get(prod, "OL_Support_Premium__c"),
        "OL_Suppt_Stan__c": get(prod, "OL_Suppt_Stan__c"),
        "P_SUPPT_STAN__c": get(prod, "P_SUPPT_STAN__c"),
        "ProductCode": get(prod, "ProductCode"),
        "Product_Category__c": get(prod, "Product_Category__c"),
        "QuantityUnitOfMeasure": get(prod, "QuantityUnitOfMeasure"),
        "Quantity_is_term__c": get(prod, "Quantity_is_term__c"),
        "StockKeepingUnit": get(prod, "StockKeepingUnit"),
        "Support__c": get(prod, "Support__c"),
        "SystemModstamp": now_iso,
        "Term__c": get(prod, "Term__c"),
        "WMS_Support__c": get(prod, "WMS_Support__c"),
        "X1YR_OM_SO_WARRANTY__c": get(prod, "X1YR_OM_SO_WARRANTY__c"),
        "X2YR_OM_SO_WARRANTY__c": get(prod, "X2YR_OM_SO_WARRANTY__c"),
        "X3YR_OM_SO_WARRANTY__c": get(prod, "X3YR_OM_SO_WARRANTY__c"),
    }
    for h in PRODUCT_HEADERS:
        row.setdefault(h, "")
    return row

def main():
    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    entry_rows = []
    product_map = {}

    for pb in data.get("pricebooks", []):
        for entry in pb.get("Entries", []) or []:
            entry_rows.append(entry_to_row(entry, now_iso))
            prod = entry.get("Product")
            if isinstance(prod, dict):
                pid = prod.get("Id")
                if pid and pid not in product_map:
                    product_map[pid] = prod

    pricebook_rows = [pricebook_to_row(pb, now_iso) for pb in data.get("pricebooks", [])]
    product_rows = [product_to_row(prod, now_iso) for prod in product_map.values()]

    with OUT_ENTRIES.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ENTRY_HEADERS, extrasaction="ignore")
        w.writeheader(); w.writerows(entry_rows)

    with OUT_PRICEBOOKS.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PRICEBOOK_HEADERS, extrasaction="ignore")
        w.writeheader(); w.writerows(pricebook_rows)

    with OUT_PRODUCTS.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PRODUCT_HEADERS, extrasaction="ignore")
        w.writeheader(); w.writerows(product_rows)

    print(f"Wrote {len(entry_rows)} entry rows -> {OUT_ENTRIES}")
    print(f"Wrote {len(pricebook_rows)} pricebooks -> {OUT_PRICEBOOKS}")
    print(f"Wrote {len(product_rows)} products -> {OUT_PRODUCTS}")

if __name__ == "__main__":
    main()
