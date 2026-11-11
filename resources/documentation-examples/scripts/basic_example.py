"""Minimal FAIRLinked example: convert a small CSV to JSON-LD."""

from pathlib import Path

import pandas as pd
from FAIRLinked import MatDatSciDf


ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "data" / "basic_example.csv"
OUTPUT_FILE = ROOT / "output" / "basic_example.jsonld"
OUTPUT_FOLDER = ROOT / "output" / "basic_example_rows"

data = pd.read_csv(INPUT_CSV)

mds_df = MatDatSciDf(
    df=data,
    df_name="basic_example",
    infer_relations=False,
    data_relations_dict={
        "mds:hasIdentifier": [("Sample", "SampleID")],
        "mds:hasHoldTemperature": [("Sample", "HoldTemperature")],
    },
)


# use serialize_bulk to create a single JSON-LD file for the entire dataset
graph = mds_df.serialize_bulk(
    output_path=str(OUTPUT_FILE),
    row_key_cols=["SampleID"],
    license="CC0-1.0",
)

# use serialize_row to create individual JSON-LD files for each row
row_graphs = mds_df.serialize_row(
    output_folder=str(OUTPUT_FOLDER),
    row_key_cols=["SampleID"],
    license="CC0-1.0",
)

print(f"Created {OUTPUT_FILE} with {len(graph)} RDF triples.")
print(f"Created {len(row_graphs)} row files in {OUTPUT_FOLDER}.")
