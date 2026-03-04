import os
import uuid
import json
import pandas as pd
import pytest
from rdflib import Graph, URIRef, RDFS, RDF, OWL, Literal
import FAIRLinked.RDFTableConversion
from FAIRLinked.RDFTableConversion import extract_data_from_csv, extract_from_folder, generate_prop_metadata_dict
from rdflib import Namespace


MDS   = Namespace("https://cwrusdle.bitbucket.io/mds/")
QUDT  = Namespace("http://qudt.org/schema/qudt/")
PROV  = Namespace("http://www.w3.org/ns/prov#")
SKOS  = Namespace("http://www.w3.org/2004/02/skos/core#")
DCTERMS = Namespace("http://purl.org/dc/terms/")



@pytest.fixture
def sample_metadata_template():
    return {
        "@context": {
            "ex": "http://example.org/",
            "prov": "http://www.w3.org/ns/prov#",
            "qudt": "http://qudt.org/schema/qudt/",
            "skos": "http://www.w3.org/2004/02/skos/core#"
        },
        "@graph": [
            {
                "@type": "ex:Sample",
                "skos:altLabel": "Value1",
                "prov:generatedAtTime": {"@value": ""},
                "qudt:hasUnit": {"@id": ""},
                "qudt:hasQuantityKind": {"@id": ""},
                "qudt:value": ""
            }
        ]
    }

@pytest.fixture
def test_template():
    path = "./test/test_data/out.jsonld" 
    with open(path, "r") as f:
        metadata_template = json.load(f)
        return metadata_template

@pytest.fixture
def sample_csv(tmp_path):
    
    return "./test/test_data/XRD_data_demo_valid.csv"


@pytest.fixture
def sample_ontology_graph():
    g = Graph()
    ex = Namespace("http://example.org/")

    # Object property
    obj_prop = ex.hasFriend
    g.add((obj_prop, RDF.type, OWL.ObjectProperty))
    g.add((obj_prop, RDFS.label, Literal("has friend")))

    # Datatype property
    dt_prop = ex.hasAge
    g.add((dt_prop, RDF.type, OWL.DatatypeProperty))
    g.add((dt_prop, RDFS.label, Literal("has age")))

    return g


def test_extract_data_from_csv_basic(test_template, sample_metadata_template, sample_csv, tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    print(output_dir)
    
    print("Function location:", extract_data_from_csv)
    print("Function file:", extract_data_from_csv.__code__.co_filename)

    print("output path test", sample_csv)

    print(test_template)
    results = extract_data_from_csv(
        metadata_template=test_template,
        csv_file=str(sample_csv),
        row_key_cols=["Value1"],
        orcid="0009-0008-4355-0543",
        output_folder=str(output_dir)
    )

    print("results", results)

    assert isinstance(results, list)
    assert len(list(output_dir.glob("*.jsonld")))>0 #should create some output files 
    for g in results:
        assert isinstance(g, Graph)
        assert len(g) > 0  # should contain some triples

def test_extract_data_with_properties_all_keys(
    test_template, sample_csv, tmp_path, sample_ontology_graph 
):
    output_dir = tmp_path / "output_props"
    output_dir.mkdir()

    print("sample csv: ", sample_csv)

    results = extract_data_from_csv(
        metadata_template=test_template,
        csv_file=str(sample_csv),
        row_key_cols=["Value1"],
        orcid="0009-0008-4355-0543",
        output_folder=str(output_dir),
        ontology_graph=sample_ontology_graph,
        )

    assert isinstance(results, list) and len(results) > 0
    jsonld_files = list(output_dir.glob("*.jsonld"))
    assert len(jsonld_files) > 0

    print("output directory: ", output_dir)

    # Check predicates exist (in-memory)
    EX = Namespace("http://example.org/")
    assert any((None, MDS.hasStudyStage, None) in g for g in results), \
        "mds:hasStudyStage missing in memory graph"

    assert any((None, QUDT.value, None) in g for g in results), \
        "qudt:value missing in memory graph"

    assert any((None, PROV.generatedAtTime, None) in g for g in results), \
        "prov:generatedAtTime missing in memory graph"

    assert any((None, SKOS.altLabel, None) in g for g in results), \
        "skos:altLabel missing in memory graph"



@pytest.mark.parametrize(
    "license_input,expected_uri",
    [
        ("MIT", "https://spdx.org/licenses/MIT.html"),  # short ID -> SPDX URI
        ("https://spdx.org/licenses/MIT.html", "https://spdx.org/licenses/MIT.html"),  # full URI
    ],
)
def test_extract_data_with_license(test_template, sample_csv, tmp_path, license_input, expected_uri):
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    results = extract_data_from_csv(
        metadata_template=test_template,
        csv_file=str(sample_csv),
        row_key_cols=["Value1"],
        orcid="0009-0008-4355-0543",
        output_folder=str(output_dir),
        license=license_input
    )

    base_uri = "https://cwrusdle.bitbucket.io/mds/"

    if 'base_uri' not in locals():
        base_uri = "https://cwrusdle.bitbucket.io/mds/"
    else:
        base_uri = locals()['base_uri']


    # Check that results is a list of RDF graphs
    assert isinstance(results, list)
    assert all(isinstance(g, Graph) for g in results)
    # Check that JSON-LD files were created
    assert len(list(output_dir.glob("*.jsonld"))) > 0

    out_file = output_dir / "dataset_license.jsonld"
    assert out_file.exists(), "dataset_license.jsonld was not created"
    data = json.loads(out_file.read_text(encoding="utf-8"))
    
    # Top-level context present
    assert "@context" in data
    assert data["@context"].get("mds") == base_uri
    assert data["@context"].get("dcterms") == "http://purl.org/dc/terms/"

    assert any((None,DCTERMS.license , None) in g for g in results), \
        "Missing license information"

@pytest.fixture
def complex_sample_csv(tmp_path):
    csv_path = tmp_path / "complex_data.csv"
    # Row 0: Header
    # Row 1: Ontology types
    # Row 1: Units
    # Row 3: Study Stage
    # Row 3+: Data
    content = (
        "Value1,AgeColumn,FriendColumn\n"
        "ex:Sample,ex:hasAge,ex:hasFriend\n"
        "unit,yr,name\n"
        "Sample,Tool,Recipe\n"
        "SampleA,25,SampleB\n"
        "SampleB,30,SampleA\n"
    )
    csv_path.write_text(content)
    return csv_path

def test_extract_data_with_complex_properties(
    sample_metadata_template, 
    complex_sample_csv, 
    tmp_path, 
    sample_ontology_graph
):
    output_dir = tmp_path / "output_complex"
    output_dir.mkdir()

    # Define the property mapping
    # "has age" is a Datatype Property in our sample_ontology_graph
    # "has friend" is an Object Property in our sample_ontology_graph
    prop_dict = {
        "has age": [("Value1", "AgeColumn")],
        "has friend": [("Value1", "FriendColumn")]
    }

    results = extract_data_from_csv(
        metadata_template=sample_metadata_template,
        csv_file=str(complex_sample_csv),
        orcid="0009-0008-4355-0543",
        output_folder=str(output_dir),
        row_key_cols=["Value1"],
        prop_column_pair_dict=prop_dict,
        ontology_graph=sample_ontology_graph
    )

    # Basic assertions
    assert len(results) == 2  # Two data rows
    jsonld_files = list(output_dir.glob("*.jsonld"))
    assert len(jsonld_files) == 2

    EX = Namespace("http://example.org/")
    
    # --- Check Datatype Property (Age) ---
    # We expect (SampleA, ex:hasAge, 25)
    age_found = False
    for g in results:
        for s, p, o in g.triples((None, EX.hasAge, None)):
            age_found = True
            assert isinstance(o, Literal)
            # Check if it caught one of our ages from the CSV
            assert str(o) in ["25", "30"]
    assert age_found, "Datatype Property 'has age' was not found in the graph"

    # --- Check Object Property (Friend) ---
    # We expect (SampleA, ex:hasFriend, SampleB_URI)
    friend_found = False
    for g in results:
        for s, p, o in g.triples((None, EX.hasFriend, None)):
            friend_found = True
            assert isinstance(o, URIRef)
            # Ensure the object is a URI, not just a string literal
            assert "http" in str(o)
    assert friend_found, "Object Property 'has friend' was not found in the graph"



