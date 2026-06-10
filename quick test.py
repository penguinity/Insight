import csv
from pathlib import Path

csv_path = Path("data/cms_source.zip") # Even if named .zip, it's our raw text file

with open(csv_path, mode="r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    # Lowercase the headers to match your normalization update
    headers = [str(k).lower() for k in reader.fieldnames]
    print("Lowercased Headers found in file:", headers)
    
    print("\nFirst 10 values for the state column:")
    for i, row in enumerate(reader):
        if i >= 10:
            break
        # Force keys to lowercase just like your loop does
        normalized_row = {str(k).lower(): v for k, v in row.items()}
        print(f"Row {i}: {normalized_row.get('rndrg_prvdr_state_abrvtn')}")