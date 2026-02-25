import os
import json
import pandas as pd
from tqdm import tqdm
import traceback
import hashlib
import re

def hash6(s):
    """
    Takes any string and returns a 6-digit number (100000-999999).
    
    Args:
        s: Input string to hash
        
    Returns:
        int: A 6-digit number between 100000 and 999999
    """
    # Create a hash of the string using SHA-256
    hash_obj = hashlib.sha256(s.encode('utf-8'))
    
    # Convert hash to integer
    hash_int = int(hash_obj.hexdigest(), 16)
    
    # Map to 6-digit range (100000-999999)
    six_digit = (hash_int % 900000) + 100000
    
    return six_digit

def extract_row_from_jsonld(data: dict, filename: str):
    row, fair_types, units, study_stages = {}, {}, {}, {}
    id_to_label = {} # This is our "Bridge"
    graph = data.get("@graph", [])

    # PASS 1: Identify "What" each column is
    for item in graph:
        label = item.get("skos:altLabel", "").strip()
        if label:
            node_id = item.get("@id")
            id_to_label[node_id] = label
            
            # Store metadata
            fair_types[label] = item.get("@type", "")
            unit = item.get("qudt:hasUnit", "")
            units[label] = unit.get("@id", "") if isinstance(unit, dict) else str(unit)
            study_stages[label] = item.get("mds:hasStudyStage", "")

    # PASS 2: Find "How much" (the actual data)
    for item in graph:
        node_id = item.get("@id")
        # Look up if this ID belongs to one of our discovered labels
        label = id_to_label.get(node_id)
        
        if label:
            val = item.get("qudt:value")
            # Handle the @value nesting and potential lists
            if isinstance(val, dict):
                val = val.get("@value", "")
            elif isinstance(val, list):
                # If you want the whole list in one cell:
                val = [v.get("@value", v) if isinstance(v, dict) else v for v in val]
            
            # Only update if there is a value (don't overwrite with empty)
            if val is not None and val != "":
                row[label] = val

    # Final logic to ensure columns exist
    row["__source_file__"] = filename
    for label in fair_types.keys():
        row.setdefault(label, "")
    fair_types["__source_file__"] = ""
    units["__source_file__"] = ""
    study_stages["__source_file__"] = ""
    return row, fair_types, units, study_stages

def jsonld_directory_to_csv(input_dir: str, output_basename: str = "merged_output", output_dir: str = "outputs", row_key: str = None):
    """
    Converts a directory of JSON-LD files into a tabular format (CSV, Parquet, Arrow).
    Each row represents a JSON-LD file with:
      - Column headers from skos:altLabel
      - Values from qudt:value
      - Extra header rows for FAIR type (@type) and units (qudt:hasUnit)

    Args:
        input_dir (str): Directory containing JSON-LD files.
        output_basename (str): Base name for output files.
        output_dir (str): Output directory to save CSV, Parquet, and Arrow files.
    """
    os.makedirs(output_dir, exist_ok=True)

    data_rows = []
    fair_type_rows = []
    unit_rows = []
    sstage_rows = []
    row_keys = ["","",""]

    for root, _, files in os.walk(input_dir):
        jsonld_files = [f for f in files if f.endswith(".jsonld")]

        for filename in tqdm(jsonld_files, desc="Processing JSON-LD files"):
            path = os.path.join(root, filename)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                row, fair_types, units, study_stages = extract_row_from_jsonld(data, filename)
                key = next((item.get("mds:row") for item in data.get("@graph", []) if "mds:row" in item), "")
                row_keys.append(key)
                data_rows.append(row)
                fair_type_rows.append(fair_types)
                unit_rows.append(units)
                sstage_rows.append(study_stages)
            except Exception as e:
                print(f"❌ Error parsing {filename}: {e} ")
                traceback.print_exc()

    if not data_rows:
        print("⚠️ No valid JSON-LD files found.")
        return

    # Create dataframes
    df = pd.DataFrame(data_rows)
    fair_df = pd.DataFrame(fair_type_rows)
    unit_df = pd.DataFrame(unit_rows)
    sstage_df = pd.DataFrame(sstage_rows)

    # Reorder columns alphabetically, placing __source_file__ at the end
    cols = [col for col in df.columns if col != "__source_file__"]
    cols.sort()
    final_cols = cols + ["__source_file__"]

    df = df[final_cols]
    fair_df = fair_df[final_cols]
    unit_df = unit_df[final_cols]
    sstage_df = sstage_df[final_cols]

    # Add header rows for type and units
    df_with_headers = pd.concat([fair_df.iloc[[0]], unit_df.iloc[[0]],sstage_df.iloc[[0]], df], ignore_index=True)

    # Add label column
    df_with_headers.insert(0, '__Label__', ['Type', 'Units', 'Study Stage'] + [str(i) for i in range(1, len(df_with_headers) - 2)])

 
    # Add row keys
    df_with_headers["__rowkey__"] = row_keys


    # Define output paths
    csv_path = os.path.join(output_dir, f"{output_basename}.csv")
    parquet_path = os.path.join(output_dir, f"{output_basename}.parquet")
    arrow_path = os.path.join(output_dir, f"{output_basename}.arrow")

    df_storage = df.astype(str)

    # Save outputs
    df_with_headers.to_csv(csv_path, index=False)
    df_storage.to_parquet(parquet_path, index=False)
    df_storage.to_feather(arrow_path)

    print(f"\n✅ Output files saved to:\n- {csv_path}\n- {parquet_path}\n- {arrow_path}")
