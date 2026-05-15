
import json
import pytest
from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal
from FAIRLinked.RDFTableConversion import extract_data_from_csv
from unittest.mock import patch
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper import prompt_for_missing_fields, jsonld_template_generator


@pytest.fixture
def sample_metadata_template():

    path = "./test/test_data/out.jsonld"
    with open (path, "r") as f:
        template = json.load(f)

        return template


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
    return load_mds_ontology_graph()

def test_extract_data_from_csv_basic(sample_metadata_template, sample_csv, tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    results = extract_data_from_csv(
        metadata_template=sample_metadata_template,
        csv_file=str(sample_csv),
        row_key_cols=["Value1"],
        orcid="0009-0008-4355-0543",
        output_folder=str(output_dir)
    )

    assert isinstance(results, list)
    assert len(list(output_dir.glob("*.jsonld"))) > 0
    for g in results:
        assert isinstance(g, Graph)
        assert len(g) > 0  # should contain some triples


def test_extract_data_with_properties(sample_metadata_template, sample_csv, tmp_path, sample_ontology_graph):
    output_dir = tmp_path / "output_props"
    output_dir.mkdir()

    prop_dict = {
        "has friend": [("Value1", "Value1")],  # trivial self-link
        "has age": [("Value1", "Value1")]
    }

    results = extract_data_from_csv(
        metadata_template=sample_metadata_template,
        csv_file=str(sample_csv),
        row_key_cols=["Value1"],
        orcid="0009-0008-4355-0543",
        output_folder=str(output_dir),
        prop_column_pair_dict=prop_dict,
        ontology_graph=sample_ontology_graph
    )

    assert len(results) > 0
    assert len(list(output_dir.glob("*.jsonld"))) > 0






@pytest.fixture
def mock_units():
    """Returns a subset of QUDT-like data for controlled testing"""
    return {
        'DEG_C': {'label': 'Celsius', 'ucum_code': 'Cel'},
        'M': {'label': 'Meter', 'ucum_code': 'm'},
        'PA': {'label': 'Pascal', 'ucum_code': 'Pa'}
    }

# --- Test 1: Standard successful input ---
def test_prompt_standard(mock_units):
    # Inputs: 1. Unit Label, 2. Study Stage, 3. Notes
    mock_inputs = ['Celsius', 'Recipe', 'Initial sample notes']
    
    with patch('builtins.input', side_effect=mock_inputs):
        with patch('builtins.print'):
            unit, stage, notes = prompt_for_missing_fields('col', None, None, None, mock_units)
            
    assert unit == 'DEG_C'
    assert stage == 'Recipe'
    assert notes == 'Initial sample notes'

# --- Test 2: Retry logic for units and study stages ---
@pytest.mark.parametrize("invalid_unit_count", [1, 2])
def test_retry_logic(invalid_unit_count, mock_units):
    # Inputs: some junk units, then a real UCUM code, then a junk stage, then real stage, then notes
    mock_inputs = (['garbage'] * invalid_unit_count) + ['Pa', 'not-a-stage', 'Synthesis', 'some note']

    with patch('builtins.input', side_effect=mock_inputs):
        with patch('builtins.print'):
            unit, stage, notes = prompt_for_missing_fields('col', None, None, None, mock_units)
            
    assert unit == 'PA'
    assert stage == 'Synthesis'

# --- Test 3: Defaulting to UNITLESS via 'exit' or 'enter' ---
@pytest.mark.parametrize("exit_command", ['exit', 'stop', 'skip', ''])
def test_unitless_fallback(exit_command, mock_units):
    # User exits unit search immediately, then hits enter for stage and notes
    mock_inputs = [exit_command, '', '']
    
    with patch('builtins.input', side_effect=mock_inputs):
        with patch('builtins.print'):
            unit, stage, notes = prompt_for_missing_fields('col', None, None, None, mock_units)
            
    assert unit == 'UNITLESS'
    assert stage == '' # Empty string is a valid member of your valid_study_stages

# --- Test 4: Integration with JSON-LD Template Generator ---
def test_jsonld_template_generator(sample_csv, sample_ontology_graph, mock_units):
    # Mock enough inputs for every column in the CSV (Unit, Stage, Notes)
    # Using 'm' (UCUM) which maps to 'M' in our mock_units
    mock_inputs = ['m', 'Result', 'auto-generated'] * 20 

    out = 'test/out/template_generator/out/out.json'
    matched = 'test/out/template_generator/log/matched.txt'
    unmatched = 'test/out/template_generator/log/unmatched.txt'

    with patch('builtins.input', side_effect=mock_inputs):
        with patch('builtins.print'):
            # Ensure your generator passes mock_units to the prompt function
            jsonld_template_generator(sample_csv, sample_ontology_graph, out, matched, unmatched)

# --- Test 5: Validation of Generated JSON-LD Content ---
def test_template_generator_output_integrity(tmp_path, sample_ontology_graph, sample_csv, mock_units):
    # Setup paths
    out_file = tmp_path / "out.json"
    
    # Inputs to choose 'Meter' (M) for every column
    mock_inputs = ['Meter', 'Analysis', 'notes'] * 20
    
    graph = sample_ontology_graph
    bindings_dict = {prefix: str(namespace) for prefix, namespace in graph.namespaces()}

    with patch('builtins.input', side_effect=mock_inputs):
        with patch('builtins.print'):
            jsonld_template_generator(sample_csv, graph, str(out_file), "matched.txt", "unmatched.txt")
    
    with open(out_file, 'r') as f:
        doc = json.load(f)
        
        for item in doc.get('@graph', []):
            # Check Unit Mapping
            unit_id = item['qudt:hasUnit']['@id'] # e.g. "unit:M"
            prefix, unit_key = unit_id.split(":")
            
            assert prefix in bindings_dict, f"Prefix {prefix} not in graph namespaces"
            assert (unit_key in mock_units or unit_key == "UNITLESS"), f"Unit {unit_key} not in QUDT mock"
            
            # Check Study Stage
            assert item.get('study_stage') == 'Analysis'







