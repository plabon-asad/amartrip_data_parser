import csv
import certifi
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId

CSV_FILE = "v6_01_03_26.csv"
FAILED_CSV = "failed_rows.csv"
# Insert data in Dhaka Metropolitan 67b5e4fee721e241d18d94a6
PARENT_ID = ObjectId('67b5e4fee721e241d18d94a6')
VERSION = "4.4.10"

client = MongoClient(
    "mongodb+srv://volume00009:71KqRv3yiKAPdMBV@amartripcluster.yph09.mongodb.net/",
    tls=True,
    tlsCAFile=certifi.where()
)

db = client["test"]
collection = db["backup_locations"]


def contains_bangla(text):
    for ch in text:
        if '\u0980' <= ch <= '\u09FF':
            return True
    return False


def save_failed_row(row, fieldnames, reason):
    file_exists = True
    try:
        open(FAILED_CSV, 'r').close()
    except FileNotFoundError:
        file_exists = False

    with open(FAILED_CSV, "a", newline="", encoding="utf-8") as f:
        all_fields = list(fieldnames) + ["fail_reason"]
        writer = csv.DictWriter(f, fieldnames=all_fields)

        if not file_exists:
            writer.writeheader()

        row2 = dict(row)
        row2["fail_reason"] = reason
        writer.writerow(row2)


def process_csv():
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    total = len(rows)
    print(f"\n📊 Total CSV Rows: {total}")

   
    print("⏳ Loading existing coordinates from DB...")
    existing_coords = set()

    # for doc in collection.find({}, {"long_lat": 1}):
    #     ll = doc.get("long_lat")
    #     if ll:
    #         existing_coords.add((round(ll[0], 15), round(ll[1], 15)))

    # print(f"   ✔ Already in DB: {len(existing_coords)} coordinates")

    inserted = 0
    skipped = 0
    failed = 0

    bulk_buffer = []
    BULK_LIMIT = 1000  

    for row in rows:
        name = row.get("name", "")
        long_str = row.get("Longitude", "").strip()
        lat_str = row.get("Latitude", "").strip()

        
        if not long_str or not lat_str:
            print(f"⚠️ FAILED (empty lat/long): {name}")
            save_failed_row(row, fieldnames, "empty lat/long")
            failed += 1
            continue

        try:
            longitude = float(long_str)
            latitude = float(lat_str)
        except:
            print(f"⚠️ FAILED (invalid lat/long): {name}")
            save_failed_row(row, fieldnames, "invalid lat/long")
            failed += 1
            continue

        if contains_bangla(name):
            longitude += 1e-17
            latitude += 1e-17

        key = (round(longitude, 15), round(latitude, 15))

        # Duplicate check from RAM (super fast)
        # if key in existing_coords:
        #     print(f"⏭️ SKIP: {name}")
        #     skipped += 1
        #     continue

        existing_coords.add(key)

        now = datetime.utcnow()

        doc = {
            "name": name,
            "type": "union",
            "long_lat": [longitude, latitude],
            "parent": PARENT_ID,
            "landmark": True,
            "areaType": row.get("AreaType", "").lower().strip(),
            "address": row.get("Address", ""),
            "road": row.get("Road", ""),
            "blockName": row.get("Block Name", ""),
            "laneName": row.get("Lane Name", ""),
            "sector": row.get("Sector", ""),
            "area": row.get("Area", ""),
            "postCode": row.get("Post Code", ""),
            "subName": row.get("subName", ""),
            "version": VERSION,
            "createdAt": now,
            "updatedAt": now,
        }

        bulk_buffer.append(doc)

        if len(bulk_buffer) >= BULK_LIMIT:
            try:
                collection.insert_many(bulk_buffer, ordered=False)
                inserted += len(bulk_buffer)
                print(f"🚀 Bulk Inserted: {inserted}/{total}")
            except Exception as e:
                print("❌ Bulk Insert Error:", e)

                for d in bulk_buffer:
                    try:
                        collection.insert_one(d)
                        inserted += 1
                    except Exception as err:
                        failed += 1
                        save_failed_row(d, fieldnames, str(err))
                        print(f"⚠️ Failed (individual): {d.get('name')}")

            bulk_buffer = []

    if bulk_buffer:
        try:
            collection.insert_many(bulk_buffer, ordered=False)
            inserted += len(bulk_buffer)
        except Exception as e:
            print("❌ Bulk Final Error:", e)
            for d in bulk_buffer:
                try:
                    collection.insert_one(d)
                    inserted += 1
                except Exception as err:
                    failed += 1
                    save_failed_row(d, fieldnames, str(err))

    print("\n" + "="*50)
    print("🎉 ALL DONE!")
    print(f"   ✅ Inserted: {inserted}")
    print(f"   ⏭️ Skipped: {skipped}")
    print(f"   ❌ Failed : {failed}")
    print("="*50)


if __name__ == "__main__":
    process_csv()
