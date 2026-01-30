import os
import json
import pandas as pd
import pytest
import rdflib
from rdflib import Graph, Namespace, RDF, RDFS, OWL, URIRef, Literal
import sys
import FAIRLinked.RDFTableConversion
from FAIRLinked.RDFTableConversion import extract_data_from_csv, extract_from_folder
from unittest.mock import patch, MagicMock
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import json

from FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper import prompt_for_missing_fields, normalize, jsonld_template_generator, extract_qudt_units


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






units = extract_qudt_units()

@pytest.fixture
def standard_mock_kinds():
    return {
        'temperature': ['celsius', 'fahrenheit'],
        'pressure': ['pascal']
    }

# test prompting for user input
def test_prompt_standard(standard_mock_kinds):
    mock_inputs = ['temperature', 'celsius', 'recipe', 'p']
    
    with patch('FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper.extract_quantity_kinds', return_value=standard_mock_kinds):
        with patch('builtins.input', side_effect=mock_inputs):
            with patch('builtins.print'):
                prompt_for_missing_fields('col', 'stage', None, None, units )


# test invalid inputs forces multiple retries
@pytest.mark.parametrize("invalid_count,valid_type", [
    (1, 'temperature'),
    (2, 'pressure'),
    (3, 'mass'),
])
def test_retry_logic(invalid_count, valid_type):
    mock_kinds = {valid_type: ['unit1']}
    mock_inputs = ['invalid'] * invalid_count + [valid_type, 'unit1', 'recipe','pushing']

    
    with patch('FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper.extract_quantity_kinds', return_value=mock_kinds):
        with patch('builtins.input', side_effect=mock_inputs):
            with patch('builtins.print'):
                prompt_for_missing_fields('col', 'stage', None, None,units)

#test valid study stage
def test_valid_study_stage(standard_mock_kinds):
    mock_inputs = ['temperature', 'celsius', 'blob', 'invalid', 'recipe', 'p']
    
    with patch('FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper.extract_quantity_kinds', return_value=standard_mock_kinds):
        with patch('builtins.input', side_effect=mock_inputs):
            with patch('builtins.print'):
                prompt_for_missing_fields('col', 'stage', None, None,units)




#standard input for template generator
def test_jsonld_template_generator(sample_csv, standard_mock_kinds):
    
    mock_inputs = ['temperature', 'celsius', 'recipe', 'p'] * 12

    data = sample_csv 
    out = 'test/out/template_generator/out/out.jsonld'
    matched = 'test/out/template_generator/log/matched.txt'
    unmatched = 'test/out/template_generator/log/unmatched.txt'
    op = load_mds_ontology_graph()

    with patch('FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper.extract_quantity_kinds', return_value=standard_mock_kinds):
        with patch('builtins.input', side_effect=mock_inputs):
            with patch('builtins.print'):
                jsonld_template_generator(data, op, out, matched, unmatched )



# test template generator uses correct units
def test_template_generator_with_extracted_units(tmp_path, sample_ontology_graph, sample_csv):
    
    mock_inputs = ['width', 'IN', 'recipe', 'p'] * 12
    graph  = load_mds_ontology_graph()
    units = extract_qudt_units()
    bindings_dict = {prefix: str(namespace) for prefix, namespace in graph.namespaces()}

    data = sample_csv #'../fairlinked/data/xrd_mock_data/XRD_data_demo_valid.csv'
    out =  tmp_path / "test/out/template_generator/out/out.jsonld"
    matched = tmp_path / "test/out/template_generator/log/matched.txt"
    unmatched = tmp_path / "test/out/template_generator/log/matched.txt"
    op = sample_ontology_graph

    with patch('builtins.input', side_effect=mock_inputs):
        with patch('builtins.print'):
            jsonld_template_generator(data, op, out, matched, unmatched )
    
    success = 1

    with open(out, 'r') as f:
        doc = json.load(f)
        
        context = doc['@context']
        for i in doc['@graph']:
            s = i['qudt:hasUnit']['@id']
            t = i['@id']
            print("id: ", t) 
            tags = t.split(":")
            frags = s.split(":")
            if frags[0] not in bindings_dict:
                success = 0
                print("fail here one")
            if frags[1] not in units:
                success = 0
                print("fail here two")
                print("missed units ", frags[1])
            if tags[0] not in bindings_dict:
                success = 0
                print("fail here three")
    assert success == 1







