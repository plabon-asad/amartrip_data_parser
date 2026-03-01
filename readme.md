## 📦 Bulk Data Import (CSV → MongoDB)

### Project config and run

- Python3 install and check version `python3 -V`
- Install pip3: `sudo apt update`, `sudo apt install python3-pip` and check version `pip3 -V`
- Install virtual environment module: `sudo apt install python3.12-venv`
- Create venv: `python3 -m venv myvenv`
- Activate venv: `source myvenv/bin/activate`
- Install required package `pip install pymongo certifi`
- Run Your Script: `python main_script.py`

## 📋 Data Import Requirements

- Data is prepared in Google Sheets
- Field names are case-sensitive
- All required fields are present
- Coordinates (Longitude, Latitude) are valid
- Exported as CSV format
- Parent ObjectId is correct
- A unique `version` is set for the import

After running:
- Check inserted count
- Review `failed_rows`
- Fix errors if needed and re-run


## Promt for AI
I am working on a ride-based location management system.

Project Stack:
- Backend: NestJS
- Database: MongoDB Atlas
- Collection: test.backup_locations
- Search: MongoDB Atlas Search (index name: autocomplete_index)
- Geo queries: 2dsphere index on long_lat
- Data import: Python CSV to MongoDB bulk insert script

System Architecture:

1) Data Import Flow
- CSV file contains: name, Longitude, Latitude, AreaType, Address, etc.
- Python script validates coordinates
- Converts lat/long to float
- Assigns parent ObjectId
- Sets type (union)
- Adds timestamps
- Bulk inserts into MongoDB Atlas
- Logs failed rows

2) MongoDB Structure
Each document contains:
{
  name,
  type (district | upazila | union),
  long_lat: [longitude, latitude],
  parent: ObjectId,
  landmark: boolean,
  areaType,
  address,
  version,
  createdAt,
  updatedAt
}

Indexes:
- 2dsphere index on long_lat
- Atlas Search index: autocomplete_index
- normal indexes on name and sorting fields

3) API Endpoints
- GET /location
- GET /location/location-near-me
- GET /location/popular-location
- GET /location/search
- POST /location/missing-location

4) Search Behavior
- Uses $search (Atlas Search)
- Fuzzy search on name, district, upazila
- Boosts popular locations
- Boosts exact name matches
- Boosts nearby results if lat/lng provided
- Expands hierarchy:
   - If district matched → return upazilas
   - If upazila matched → return unions

5) Geo Query
- Uses $near with 2dsphere index
- Max distance: 10km

6) Missing Location
- Users can submit missing locations
- Stored with PENDING status

Now I want help with: