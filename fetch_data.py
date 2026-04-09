#!/usr/bin/env python3
"""
UpDish data fetcher
Pulls food/beverage business licences from City of Vancouver Open Data
and writes data.js for the map.

Run: python3 fetch_data.py
Output: data.js (drop this in /root/mysite/html/updish/)
"""

import urllib.request
import json
import time
import os

API_BASE = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/business-licences/records"

# Business types to include
FOOD_TYPES = [
    "Restaurant",
    "Cafeteria",
    "Coffee Bar/Tea House",
    "Pub",
    "Tavern/Lounge",
    "Catering",
    "Food Processor - Retail",
]

# Map CoV types to our display types
TYPE_MAP = {
    "Restaurant": "restaurant",
    "Cafeteria": "restaurant",
    "Coffee Bar/Tea House": "cafe",
    "Pub": "bar",
    "Tavern/Lounge": "bar",
    "Catering": "food_truck",
    "Food Processor - Retail": "cafe",
}

BENCHMARKS = [
    {"stat": "31–34%", "desc": "avg food cost ratio for BC independents", "source": "Restaurants Canada 2026"},
    {"stat": "~62%", "desc": "of new food licences survive 3+ years in Vancouver", "source": "BC Stats 2025"},
    {"stat": "$42k", "desc": "median annual revenue per seat, full-service", "source": "TouchBistro 2025"},
    {"stat": "3.4×", "desc": "Kitsilano café density vs city average", "source": "CoV Open Data"},
    {"stat": "~8%", "desc": "net margin for healthy independent café", "source": "Restaurants Canada"},
    {"stat": "28–32%", "desc": "target labour cost as % of revenue", "source": "TouchBistro 2025"},
]


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
        f"&fields=businessname,businesstradename,businesstype,status,numberofemployees,"
        f"house,street,localarea,postalcode,geo_point_2d"
    )
    url = f"{API_BASE}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "UpDish/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def main():
    import urllib.parse  # needed inside fetch_page

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
                continue  # skip if no coordinates

            lat = geo.get("lat")
            lon = geo.get("lon")
            if not lat or not lon:
                continue

            name = r.get("businesstradename") or r.get("businessname") or "Unknown"
            btype = r.get("businesstype", "")
            address = " ".join(filter(None, [r.get("house", ""), r.get("street", "")])).strip()
            neighbourhood = r.get("localarea") or "Vancouver"
            employees = r.get("numberofemployees")
            employees = int(employees) if employees else 0
            status = "active" if r.get("status") == "Issued" else "inactive"
            display_type = TYPE_MAP.get(btype, "restaurant")

            # Determine licence label
            if btype in ("Pub", "Tavern/Lounge"):
                licence = "Liquor Primary"
            else:
                licence = "Food Primary"

            businesses.append({
                "name": name,
                "address": address,
                "neighbourhood": neighbourhood,
                "type": display_type,
                "employees": employees,
                "licence": licence,
                "status": status,
                "lat": lat,
                "lng": lon,
            })

        offset += limit
        if offset >= total:
            break

        time.sleep(0.2)  # be polite to the API

    print(f"\nFetched {len(businesses)} businesses with coordinates.")

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
    print("Done. Copy data.js to /root/mysite/html/updish/ if not already there.")


if __name__ == "__main__":
    import urllib.parse
    main()
