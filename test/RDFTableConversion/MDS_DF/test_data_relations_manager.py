import json
import os
import warnings
import pytest
import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, OWL
from FAIRLinked.RDFTableConversion.MDS_DF.data_relations_manager import DataRelationsDict


"""
Tests for data_relations_manager.py — the DataRelationsDict class.

All tests are self-contained: ontology graphs are built in-memory using rdflib
so that no external files or network connections are required.
"""


# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------

MDS_NS  = Namespace("https://cwrusdle.bitbucket.io/mds/")
QUDT_NS = Namespace("http://qudt.org/schema/qudt/")


def _build_ontology(props: list[tuple[str, str, str]]) -> Graph:
    """
    Build a tiny in-memory ontology graph.

    ``props`` is a list of (label, uri_local, prop_type) where
    ``prop_type`` is either "ObjectProperty" or "DatatypeProperty".
    """
    g = Graph()
    g.bind("mds", MDS_NS)
    for label, local, ptype in props:
        uri = MDS_NS[local]
        owl_type = OWL.ObjectProperty if ptype == "ObjectProperty" else OWL.DatatypeProperty
        g.add((uri, RDF.type, owl_type))
        g.add((uri, RDFS.label, Literal(label)))
    return g


def _onto_props_dict(g: Graph) -> dict:
    """Mimic the output of MatDatSciDf.get_relations() for a given graph."""
    result = {}
    for ptype, label in [
        (OWL.ObjectProperty, "Object Property"),
        (OWL.DatatypeProperty, "Datatype Property"),
    ]:
        for prop in g.subjects(RDF.type, ptype):
            lbl = g.value(prop, RDFS.label)
            if lbl:
                result[str(lbl)] = (str(prop), label)
    return result


@pytest.fixture
def simple_ontology():
    """Ontology with one object property and one datatype property."""
    return _build_ontology([
        ("measuredBy",   "measuredBy",   "ObjectProperty"),
        ("hasValue",     "hasValue",     "DatatypeProperty"),
    ])


@pytest.fixture
def onto_props(simple_ontology):
    return _onto_props_dict(simple_ontology)


@pytest.fixture
def simple_df():
    return pd.DataFrame({
        "Temperature": [100, 200],
        "Sensor_ID":   ["S1", "S2"],
        "Value":       [1.0, 2.0],
    })


@pytest.fixture
def drd():
    return DataRelationsDict(prop_col_pair_dict={})



class TestDataRelationsDictInit:
    def test_empty_dict(self, drd):
        assert drd.prop_pair_dict == {}

    def test_pre_populated_dict(self):
        initial = {"mds:measuredBy": [("Temperature", "Sensor_ID")]}
        d = DataRelationsDict(prop_col_pair_dict=initial)
        assert "mds:measuredBy" in d.prop_pair_dict


class TestAddRelations:
    def test_adds_new_property_by_label(self, drd, simple_ontology, onto_props):
        drd.add_relations(
            {"measuredBy": [("Temperature", "Sensor_ID")]},
            ontology_graph=simple_ontology,
            onto_props=onto_props,
        )
        assert "measuredBy" in drd.prop_pair_dict
        assert ("Temperature", "Sensor_ID") in drd.prop_pair_dict["measuredBy"]

    def test_adds_new_property_by_curie(self, drd, simple_ontology, onto_props):
        drd.add_relations(
            {"mds:measuredBy": [("Temperature", "Sensor_ID")]},
            ontology_graph=simple_ontology,
            onto_props=onto_props,
        )
        assert "mds:measuredBy" in drd.prop_pair_dict

    def test_extends_existing_key(self, drd, simple_ontology, onto_props):
        drd.prop_pair_dict["measuredBy"] = [("Temperature", "Sensor_ID")]
        drd.add_relations(
            {"measuredBy": [("Value", "Sensor_ID")]},
            ontology_graph=simple_ontology,
            onto_props=onto_props,
        )
        pairs = drd.prop_pair_dict["measuredBy"]
        assert ("Temperature", "Sensor_ID") in pairs
        assert ("Value", "Sensor_ID") in pairs

    def test_unknown_property_raises_warning(self, drd, simple_ontology, onto_props):
        with pytest.warns(UserWarning, match="not defined in the loaded ontology"):
            drd.add_relations(
                {"unknownProp": [("Temperature", "Sensor_ID")]},
                ontology_graph=simple_ontology,
                onto_props=onto_props,
            )
        # Still added despite the warning
        assert "unknownProp" in drd.prop_pair_dict

    def test_multiple_properties_added_at_once(self, drd, simple_ontology, onto_props):
        drd.add_relations(
            {
                "measuredBy": [("Temperature", "Sensor_ID")],
                "hasValue":   [("Value", "Temperature")],
            },
            ontology_graph=simple_ontology,
            onto_props=onto_props,
        )
        assert "measuredBy" in drd.prop_pair_dict
        assert "hasValue" in drd.prop_pair_dict


class TestValidateDataRelations:
    def test_valid_returns_true(self, simple_ontology, onto_props, simple_df):
        d = DataRelationsDict({"measuredBy": [("Temperature", "Sensor_ID")]})
        result = d.validate_data_relations(simple_df, simple_ontology, onto_props)
        assert result is True

    def test_missing_subject_col_returns_false(
        self, simple_ontology, onto_props, simple_df
    ):
        d = DataRelationsDict({"measuredBy": [("NonExistent", "Sensor_ID")]})
        result = d.validate_data_relations(simple_df, simple_ontology, onto_props)
        assert result is False

    def test_missing_object_col_returns_false(
        self, simple_ontology, onto_props, simple_df
    ):
        d = DataRelationsDict({"measuredBy": [("Temperature", "NoSuchCol")]})
        result = d.validate_data_relations(simple_df, simple_ontology, onto_props)
        assert result is False

    def test_invalid_property_returns_false(
        self, simple_ontology, onto_props, simple_df
    ):
        d = DataRelationsDict({"nonExistentProp": [("Temperature", "Sensor_ID")]})
        result = d.validate_data_relations(simple_df, simple_ontology, onto_props)
        assert result is False

    def test_empty_dict_returns_true(self, simple_ontology, onto_props, simple_df):
        d = DataRelationsDict({})
        result = d.validate_data_relations(simple_df, simple_ontology, onto_props)
        assert result is True

    def test_full_uri_key_resolves(self, simple_ontology, onto_props, simple_df):
        full_uri = str(MDS_NS["measuredBy"])
        d = DataRelationsDict({full_uri: [("Temperature", "Sensor_ID")]})
        result = d.validate_data_relations(simple_df, simple_ontology, onto_props)
        assert result is True


class TestPrintDataRelations:
    def test_prints_without_error_no_validation(self, drd, capsys):
        drd.prop_pair_dict = {"mds:measuredBy": [("Temperature", "Sensor_ID")]}
        drd.print_data_relations()  # no df / onto_props provided
        captured = capsys.readouterr()
        assert "mds:measuredBy" in captured.out
        assert "Temperature" in captured.out

    def test_prints_valid_marks(
        self, simple_df, simple_ontology, onto_props, capsys
    ):
        d = DataRelationsDict({"measuredBy": [("Temperature", "Sensor_ID")]})
        d.print_data_relations(
            df=simple_df,
            ontology_graph=simple_ontology,
            onto_props=onto_props,
        )
        captured = capsys.readouterr()
        assert "✅" in captured.out

    def test_prints_error_for_unknown_property(
        self, simple_df, simple_ontology, onto_props, capsys
    ):
        d = DataRelationsDict({"bogus:prop": [("Temperature", "Sensor_ID")]})
        d.print_data_relations(
            df=simple_df,
            ontology_graph=simple_ontology,
            onto_props=onto_props,
        )
        captured = capsys.readouterr()
        assert "Property Unknown" in captured.out

    def test_prints_error_for_missing_column(
        self, simple_df, simple_ontology, onto_props, capsys
    ):
        d = DataRelationsDict({"measuredBy": [("MissingCol", "Sensor_ID")]})
        d.print_data_relations(
            df=simple_df,
            ontology_graph=simple_ontology,
            onto_props=onto_props,
        )
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_empty_dict_message(self, capsys):
        d = DataRelationsDict({})
        d.print_data_relations()
        captured = capsys.readouterr()
        assert "No relations defined" in captured.out


class TestSaveRelations:
    def test_saves_json_and_txt(self, tmp_path):
        d = DataRelationsDict({"measuredBy": [("Temperature", "Sensor_ID")]})
        out = tmp_path / "relations.json"
        d.save_relations(str(out))
        assert (tmp_path / "relations.json").exists()
        assert (tmp_path / "relations.txt").exists()

    def test_json_content_correct(self, tmp_path):
        d = DataRelationsDict({"measuredBy": [("Temperature", "Sensor_ID")]})
        out = tmp_path / "relations.json"
        d.save_relations(str(out))
        loaded = json.loads((tmp_path / "relations.json").read_text())
        assert "measuredBy" in loaded
        assert loaded["measuredBy"] == [["Temperature", "Sensor_ID"]]

    def test_txt_content_contains_property(self, tmp_path):
        d = DataRelationsDict({"measuredBy": [("Temperature", "Sensor_ID")]})
        out = tmp_path / "relations.json"
        d.save_relations(str(out))
        txt = (tmp_path / "relations.txt").read_text()
        assert "measuredBy" in txt
        assert "Temperature" in txt

    def test_empty_dict_warns(self, tmp_path, capsys):
        d = DataRelationsDict({})
        d.save_relations(str(tmp_path / "empty.json"))
        captured = capsys.readouterr()
        assert "No relations defined" in captured.out

    def test_extension_stripped_from_base_path(self, tmp_path):
        d = DataRelationsDict({"measuredBy": [("Temperature", "Sensor_ID")]})
        # both .json and .txt should still be created
        out = tmp_path / "relations.txt"
        d.save_relations(str(out))
        assert (tmp_path / "relations.json").exists()
        assert (tmp_path / "relations.txt").exists()
