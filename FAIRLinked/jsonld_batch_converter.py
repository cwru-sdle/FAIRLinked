import os
import json
import pandas as pd
from tqdm import tqdm

def extract_row_from_jsonld(data: dict, filename: str):
    """
    Extracts a single row of data from a JSON-LD object.

    Args:
        data (dict): JSON-LD data parsed as dictionary.
        filename (str): Name of the file the data came from.

    Returns:
        tuple: (row_values, fair_types, units) as dictionaries.
    """
    row = {}
    fair_types = {}
    units = {}

    for item in data.get("@graph", []):
        alt_label = item.get("skos:altLabel", "").strip()
        if not alt_label:
            continue

        value = item.get("qudt:value", "")
        fair_type = item.get("@type", "")
        unit = item.get("qudt:hasUnit", "")

        row[alt_label] = value
        fair_types[alt_label] = fair_type
        units[alt_label] = unit

    row["__source_file__"] = filename
    fair_types["__source_file__"] = ""
    units["__source_file__"] = ""
    return row, fair_types, units

def jsonld_directory_to_csv(input_dir: str, output_basename: str = "merged_output", output_dir: str = "outputs"):
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

    for root, _, files in os.walk(input_dir):
        jsonld_files = [f for f in files if f.endswith(".jsonld")]

        for filename in tqdm(jsonld_files, desc="Processing JSON-LD files"):
            path = os.path.join(root, filename)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                row, fair_types, units = extract_row_from_jsonld(data, filename)
                data_rows.append(row)
                fair_type_rows.append(fair_types)
                unit_rows.append(units)
            except Exception as e:
                print(f"❌ Error parsing {filename}: {e}")

    if not data_rows:
        print("⚠️ No valid JSON-LD files found.")
        return

    # Create dataframes
    df = pd.DataFrame(data_rows)
    fair_df = pd.DataFrame(fair_type_rows)
    unit_df = pd.DataFrame(unit_rows)

    # Reorder columns alphabetically, placing __source_file__ at the end
    cols = [col for col in df.columns if col != "__source_file__"]
    cols.sort()
    final_cols = cols + ["__source_file__"]

    df = df[final_cols]
    fair_df = fair_df[final_cols]
    unit_df = unit_df[final_cols]

    # Add header rows for type and units
    df_with_headers = pd.concat([fair_df.iloc[[0]], unit_df.iloc[[0]], df], ignore_index=True)

    # Define output paths
    csv_path = os.path.join(output_dir, f"{output_basename}.csv")
    parquet_path = os.path.join(output_dir, f"{output_basename}.parquet")
    arrow_path = os.path.join(output_dir, f"{output_basename}.arrow")

    # Save outputs
    df_with_headers.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    df.to_feather(arrow_path)

    print(f"\n✅ Output files saved to:\n- {csv_path}\n- {parquet_path}\n- {arrow_path}")

# ----------------------
# USAGE SHOULD GO IN README, NOT MAIN BLOCK
# ----------------------

if __name__ == "__main__":
    print("❌ This script is intended to be imported and used in other scripts or notebooks.")
    print("   Please call jsonld_directory_to_csv(...) with appropriate paths.")
