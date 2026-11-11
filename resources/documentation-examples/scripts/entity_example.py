"""Create one sample-ID JSON-LD file for each data row.

This demo imports FAIRLinked from the active Python environment (for example,
the package installed from PyPI). The ``XraySample`` RDF entity is intentionally
not represented by a placeholder CSV column. It is uniquely resolved in
MDS-Onto and automatically registered when the relationships are added.
"""

from pathlib import Path

import pandas as pd

from FAIRLinked import MatDatSciDf


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = WORKSPACE_ROOT / "data" / "sample_id_demo.csv"
OUTPUT_PATH = WORKSPACE_ROOT / "output" / "sample_id_demo"


raw_df = pd.read_csv(CSV_PATH)

mds_df = MatDatSciDf(
    df=raw_df,
    orcid="0000-0002-9273-8488",
    df_name="sample_id_demo",
    metadata_rows=True,
    infer_relations=False,
)

mds_df.add_entity(
        name="Sample",
        rdf_type="mds:Sample",
        definition="Sample associated with this photovoltaic site",
    )


sample_relations = {
    "mds:hasIdentifier": [("Sample", "SampleID")],
    "mds:hasName": [("Sample", "SampleName")],
    "mds:hasSource": [("Sample", "Source")],
    "mds:hasComment": [("Sample", "Comment")],
    "mds:hasDate": [("Sample", "DateMade")],
    "mds:hasProject": [("Sample", "Project")],
}
mds_df.add_relations(sample_relations)

row_graphs = mds_df.serialize_row(
    output_folder=str(OUTPUT_PATH),
    format="json-ld",
    row_key_cols=["SampleID"],
    license="MIT",
)

print(f"Created {len(row_graphs)} sample JSON-LD files in {OUTPUT_PATH}")
