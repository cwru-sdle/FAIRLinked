import os
import json
import pandas as pd
from tqdm import tqdm

def extract_row_from_jsonld(data: dict, filename: str):
    row = {}
    fair_types = {}
    units = {}
    
    for item in data.get("@graph", []):
        alt_label = item.get("skos:altLabel", "").strip()
        if not alt_label:
            continue  # skip entries without altLabel

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
    Converts a directory of JSON-LD files into a single CSV/Parquet/Arrow with:
    - One row per file
    - Columns from skos:altLabel
    - Values from qudt:value
    - Extra header rows for @type and qudt:hasUnit

    Args:
        input_dir (str): Path to the directory containing JSON-LD files.
        output_basename (str): Output filename base (no extension).
        output_dir (str): Directory to save CSV, Arrow, and Parquet outputs.
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
        print("⚠️ No JSON-LD records found.")
        return

    # Create main DataFrame and reorder headers
    df = pd.DataFrame(data_rows)
    fair_df = pd.DataFrame(fair_type_rows)
    unit_df = pd.DataFrame(unit_rows)

    # Sort columns alphabetically except source file
    cols = [col for col in df.columns if col != "__source_file__"]
    cols.sort()
    final_cols = cols + ["__source_file__"]

    df = df[final_cols]
    fair_df = fair_df[final_cols]
    unit_df = unit_df[final_cols]

    # Insert the two header rows on top
    df_with_headers = pd.concat([fair_df.iloc[[0]], unit_df.iloc[[0]], df], ignore_index=True)

    # Output paths
    csv_path = os.path.join(output_dir, f"{output_basename}.csv")
    parquet_path = os.path.join(output_dir, f"{output_basename}.parquet")
    arrow_path = os.path.join(output_dir, f"{output_basename}.arrow")

    # Save outputs
    df_with_headers.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    df.to_feather(arrow_path)

    print(f"\n✅ Output files saved to:\n- {csv_path}\n- {parquet_path}\n- {arrow_path}")

# Example usage
if __name__ == "__main__":
    jsonld_directory_to_csv(
        input_dir="/Users/lambaritu67/Desktop/fairlinked/data/FAIRLinked_test_outputs/output_test_data/DS0d69555d-Manufacturer-SampleID",
        output_basename="merged_DS0d69555d",
        output_dir="outputs"
    )
