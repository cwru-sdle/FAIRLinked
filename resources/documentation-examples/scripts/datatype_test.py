"""Exercise FAIRLinked with latitude/longitude datatype properties."""

from pathlib import Path

import pandas as pd
from rdflib import Namespace
from rdflib.namespace import OWL, RDF

from FAIRLinked import MatDatSciDf


# change to your root/paths
WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
FAIRLINKED_REPO = WORKSPACE_ROOT / "github" / "FAIRLinked"
INPUT_CSV = WORKSPACE_ROOT / "data" / "datatype_test.csv"
OUTPUT_DIR = WORKSPACE_ROOT / "output" / "datatype_test"


GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")


raw_df = pd.read_csv(INPUT_CSV)

mds_df = MatDatSciDf(
    df=raw_df,
    metadata_template={},
    orcid="0000-0002-9273-8488",
    df_name="Datatype Property Test Ingestion",
    metadata_rows=True,
    infer_relations=False,
)

# adding datatype properties for latitude and longitude to the ontology graph from w3, since they are not in the mdsOnto yet
mds_df.ontology.add((GEO.lat, RDF.type, OWL.DatatypeProperty))
mds_df.ontology.add((GEO.long, RDF.type, OWL.DatatypeProperty))

neighbor_predicates = {
    "NorthNeighbor": "mds:hasNorthNeighbor",
    "NortheastNeighbor": "mds:hasNortheastNeighbor",
    "EastNeighbor": "mds:hasEastNeighbor",
    "SoutheastNeighbor": "mds:hasSoutheastNeighbor",
    "SouthNeighbor": "mds:hasSouthNeighbor",
    "SouthwestNeighbor": "mds:hasSouthwestNeighbor",
    "WestNeighbor": "mds:hasWestNeighbor",
    "NorthwestNeighbor": "mds:hasNorthwestNeighbor",
}

for geohash_column in ("Geohash", *neighbor_predicates):
    mds_df.update_metadata(
        col_name=geohash_column,
        field="type",
        value="mds:Geohash",
    )


# Using the w3 datatype properties for latitude and longitude, since they arent in mdsOnto yet
mds_df.add_relations(
    {   # mapping of datatype properties for lat/long columns in the dataframe (columns can be named anything)
        "http://www.w3.org/2003/01/geo/wgs84_pos#lat": [("Centroid", "Latitude")],
        "http://www.w3.org/2003/01/geo/wgs84_pos#long": [("Centroid", "Longitude")],

        # mapping of object properties between geohashes
        **{
            predicate: [("Geohash", neighbor_column)]
            for neighbor_column, predicate in neighbor_predicates.items()
        },
    }
)

mds_df.view_metadata()
mds_df.view_data_relations()
mds_df.validate_data_relations()

mds_df.serialize_row(
    output_folder=OUTPUT_DIR,
    format="json-ld",
    row_key_cols=["GeohashName"],
    license="MIT",
    id_cols=[
        *neighbor_predicates,
    ],
)
