#!/usr/bin/env python3
"""
UpDish data fetcher
Pulls food/beverage business licences from City of Vancouver Open Data
and writes data.js for the map.

Run: python3 fetch_data.py
Output: data.js
"""

import urllib.request
import urllib.parse
import json
import time
import os

API_BASE = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/business-licences/records"

# CoV business types to fetch
FOOD_TYPES = [
    "Restaurant",
    "Liquor Establishment",
    "Street Vendor",
    "Caterer",
]

BENCHMARKS = [
    {"stat": "31–34%", "desc": "avg food cost ratio for BC independents", "source": "Restaurants Canada 2026"},
    {"stat": "~62%", "desc": "of new food licences survive 3+ years in Vancouver", "source": "BC Stats 2025"},
    {"stat": "$42k", "desc": "median annual revenue per seat, full-service", "source": "TouchBistro 2025"},
    {"stat": "3.4×", "desc": "Kitsilano café density vs city average", "source": "CoV Open Data"},
    {"stat": "~8%", "desc": "net margin for healthy independent café", "source": "Restaurants Canada"},
    {"stat": "28–32%", "desc": "target labour cost as % of revenue", "source": "TouchBistro 2025"},
]

LIQUOR_SUBTYPES = {"Class 1 with liquor service", "Class 2 with liquor service"}


def classify(businesstype, businesssubtype):
    """Return (type_key, licence_label) for a record."""
    bt = (businesstype or "").strip()
    bs = (businesssubtype or "").strip()

    if bt == "Restaurant":
        if bs in LIQUOR_SUBTYPES:
            return "restaurant_liquor", "Food Primary + Liquor"
        else:
            return "restaurant", "Food Primary"
    elif bt == "Liquor Establishment":
        return "bar", "Liquor Primary"
    elif bt == "Street Vendor":
        return "street_vendor", "Street Vendor"
    elif bt == "Caterer":
        return "caterer", "Caterer"
    else:
        return "restaurant", "Food Primary"


def build_where_clause():
    quoted = [f"'{t}'" for t in FOOD_TYPES]
    types_str = ", ".join(quoted)
    return f"businesstype in ({types_str}) AND status='Issued'"


def fetch_page(offset, limit=100):
    where = build_where_clause()
    params = (
        f"where={urllib.parse.quote(where)}"
        f"&limit={limit}"
        f"&offset={offset}"
        f"&fields=businessname,businesstradename,businesstype,businesssubtype,status,"
        f"numberofemployees,house,street,localarea,postalcode,geo_point_2d"
    )
    url = f"{API_BASE}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "UpDish/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def main():
    print("Fetching City of Vancouver business licences...")
    businesses = []
    offset = 0
    limit = 100
    total = None

    while True:
        print(f"  Fetching records {offset}–{offset+limit}...", end=" ", flush=True)
        data = fetch_page(offset, limit)

        if total is None:
            total = data.get("total_count", 0)
            print(f"(total: {total})")
        else:
            print()

        results = data.get("results", [])
        if not results:
            break

        for r in results:
            geo = r.get("geo_point_2d")
            if not geo:
                continue
            lat = geo.get("lat")
            lon = geo.get("lon")
            if not lat or not lon:
                continue

            name = r.get("businesstradename") or r.get("businessname") or "Unknown"
            address = " ".join(filter(None, [r.get("house", ""), r.get("street", "")])).strip()
            neighbourhood = r.get("localarea") or "Vancouver"
            employees = r.get("numberofemployees")
            employees = int(employees) if employees else 0
            btype = r.get("businesstype", "")
            bsubtype = r.get("businesssubtype", "")

            type_key, licence_label = classify(btype, bsubtype)

            businesses.append({
                "name": name,
                "address": address,
                "neighbourhood": neighbourhood,
                "type": type_key,
                "employees": employees,
                "licence": licence_label,
                "status": "active",
            })

        offset += limit
        if offset >= total:
            break
        time.sleep(0.2)

    print(f"\nFetched {len(businesses)} businesses with coordinates.")

    # Type breakdown
    counts = {}
    for b in businesses:
        counts[b["type"]] = counts.get(b["type"], 0) + 1
    print("Breakdown:", json.dumps(counts, indent=2))

    # Write data.js
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.js")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("// UpDish — live data\n")
        f.write("// Source: City of Vancouver Open Data, business-licences dataset\n")
        f.write(f"// Fetched: {time.strftime('%Y-%m-%d')}\n")
        f.write(f"// Total records: {len(businesses)}\n\n")
        f.write("const BUSINESSES = ")
        f.write(json.dumps(businesses, indent=2, ensure_ascii=False))
        f.write(";\n\n")
        f.write("const BENCHMARKS = ")
        f.write(json.dumps(BENCHMARKS, indent=2))
        f.write(";\n")

    print(f"Written to {output_path}")


if __name__ == "__main__":
    main()
