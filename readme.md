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

## ⚠️ Critical ObservationsCase Sensitivity:

 1. **Case Sensitivity**: Notice that `Longitude` and `Latitude` start with **Uppercase**, while `name` and `subName` start with **lowercase**. Your CSV headers must match these exactly.
 
 2. **Mandatory Fields**: If `Longitude` or `Latitude` are missing or empty in a row, the script will skip that row and log it to `failed_rows.csv`.
 
 3. **Special Logic (Bangla)**: If the `name` column contains Bangla characters, the script adds a tiny offset ($1 \times 10^{-17}$) to the coordinates.
 
 4. **Static Fields**: Some fields are hardcoded in the script and do not need to be in your CSV

