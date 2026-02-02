import os
import json
import pandas as pd
import pytest
from pathlib import Path
import FAIRLinked.RDFTableConversion
from FAIRLinked.RDFTableConversion import jsonld_directory_to_csv

@pytest.fixture
def sample_jsonld_dir(tmp_path):
    """Creates a temporary directory with two sample JSON-LD files."""
    input_dir = tmp_path / "jsonld_input"
    input_dir.mkdir()

    #sample jsonld
    sample_data_1 = {
        "@graph": [
            {
                "skos:altLabel": "Value1",
                "qudt:value": 123,
                "@type": "ex:SampleType",
                "qudt:hasUnit": "unit:m"
            },
            {
                "skos:altLabel": "Value2",
                "qudt:value": 456,
                "@type": "ex:OtherType",
                "qudt:hasUnit": "unit:s"
            }
        ]
    }

    sample_data_2 = {
        "@graph": [
            {
                "skos:altLabel": "Value1",
                "qudt:value": 789,
                "@type": "ex:SampleType",
                "qudt:hasUnit": "unit:m"
            },
            {
                "skos:altLabel": "Value2",
                "qudt:value": 101,
                "@type": "ex:OtherType",
                "qudt:hasUnit": "unit:s"
            }
        ]
    }

    with open(input_dir / "file1.jsonld", "w", encoding="utf-8") as f:
        json.dump(sample_data_1, f)

    with open(input_dir / "file2.jsonld", "w", encoding="utf-8") as f:
        json.dump(sample_data_2, f)

    return input_dir


def test_jsonld_directory_to_csv_creates_outputs(sample_jsonld_dir, tmp_path):
    output_dir = tmp_path / "outputs"

    jsonld_directory_to_csv(
        input_dir=str(sample_jsonld_dir),
        output_basename="merged_test",
        output_dir=str(output_dir)
    )

    # Check output files exist
    csv_path = output_dir / "merged_test.csv"
    parquet_path = output_dir / "merged_test.parquet"
    arrow_path = output_dir / "merged_test.arrow"

    assert csv_path.exists(), "CSV file not created"
    assert parquet_path.exists(), "Parquet file not created"
    assert arrow_path.exists(), "Arrow file not created"

    # Check CSV structure
    df_csv = pd.read_csv(csv_path)
    assert "Value1" in df_csv.columns
    assert "Value2" in df_csv.columns
    assert "__source_file__" in df_csv.columns

    # First two rows in CSV should be FAIR type + units
    assert len(df_csv) >= 3













