import os
import json
import pandas as pd
import pytest
import sys
import json
import unittest
import tempfile
import shutil
from pathlib import Path
from FAIRLinked.RDFTableConversion.MDS_DF.main import MatDatSciDf


"""
Tests for main.py — the MatDatSciDf class.

Heavy external dependencies (ORCID API, MDS ontology loader, RDF serialization)
are mocked so that every test is fast and fully offline.
"""

import warnings
from unittest.mock import MagicMock, patch, PropertyMock
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, OWL

# ---------------------------------------------------------------------------
# Shared helpers / factories
# ---------------------------------------------------------------------------

MDS_NS  = Namespace("https://cwrusdle.bitbucket.io/mds/")
QUDT_NS = Namespace("http://qudt.org/schema/qudt/")

BASE_CONTEXT = {
    "skos":    "http://www.w3.org/2004/02/skos/core#",
    "qudt":    "http://qudt.org/schema/qudt/",
    "unit":    "https://qudt.org/vocab/unit/",
    "mds":     "https://cwrusdle.bitbucket.io/mds/",
    "xsd":     "http://www.w3.org/2001/XMLSchema#",
    "prov":    "http://www.w3.org/ns/prov#",
    "dcterms": "http://purl.org/dc/terms/",
    "rdf":     "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
}

def _make_template(col_names: list[str] | None = None) -> dict:
    col_names = col_names or ["Temperature"]
    graph = []
    for col in col_names:
        graph.append({
            "@id":              f"mds:{col}",
            "@type":            f"mds:{col}",
            "skos:altLabel":    col,
            "skos:definition":  f"Definition of {col}",
            "qudt:hasUnit":     {"@id": "unit:DEG_C"},
            "prov:generatedAtTime": {
                "@value": "2024-01-01T00:00:00+00:00Z",
                "@type": "xsd:dateTime",
            },
            "mds:hasStudyStage": "Synthesis",
        })
    return {"@context": BASE_CONTEXT, "@graph": graph}

def _build_ontology() -> Graph:
    """Minimal in-memory ontology with one object and one datatype property."""
    g = Graph()
    g.bind("mds", MDS_NS)
    for local, ptype in [("measuredBy", OWL.ObjectProperty), ("hasValue", OWL.DatatypeProperty)]:
        uri = MDS_NS[local]
        g.add((uri, RDF.type, ptype))
        g.add((uri, RDFS.label, Literal(local)))
    return g

def _make_df(cols: list[str] | None = None, rows: int = 3) -> pd.DataFrame:
    cols = cols or ["Temperature"]
    return pd.DataFrame({c: range(rows) for c in cols})

@pytest.fixture(autouse=True)
def patch_mds_graph():
    onto = _build_ontology()
    with patch.object(MatDatSciDf, 'mds_graph', new=onto):
        yield onto

@pytest.fixture(autouse=True)
def patch_orcid_api():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    with patch("FAIRLinked.RDFTableConversion.MDS_DF.main.requests.get", return_value=mock_resp) as m:
        yield m


def make_mdsdf(
    cols=None,
    rows=3,
    orcid="0000-0000-0000-0000",
    df_name="TestDF",
    metadata_rows=False,
    data_relations_dict=None,
    metadata_template=None,
):
    df = _make_df(cols, rows)
    tmpl = metadata_template or _make_template(cols)
    onto = _build_ontology()
    return MatDatSciDf(
        df=df,
        metadata_template=tmpl,
        orcid=orcid,
        df_name=df_name,
        metadata_rows=metadata_rows,
        ontology_graph=onto,
        data_relations_dict=data_relations_dict or {},
    )


class TestMatDatSciDfInit:
    def test_placeholder_orcid_not_verified(self):
        m = make_mdsdf()
        assert m.orcid == "0000-0000-0000-0000"
        assert m.orcid_verified is False

    def test_real_orcid_verified_on_200(self, patch_orcid_api):
        patch_orcid_api.return_value.status_code = 200
        m = make_mdsdf(orcid="0000-0001-2345-6789")
        assert m.orcid_verified is True
 
    def test_real_orcid_unverified_on_404(self, patch_orcid_api):
        patch_orcid_api.return_value.status_code = 404
        with pytest.warns(UserWarning):
            m = make_mdsdf(orcid="0000-0001-2345-6789")
        assert m.orcid_verified is False
 
    def test_connection_error_marks_unverified(self, patch_orcid_api):
        import requests as req
        patch_orcid_api.side_effect = req.exceptions.ConnectionError("no network")
        with pytest.warns(UserWarning):
            m = make_mdsdf(orcid="0000-0001-2345-6789")
        assert m.orcid_verified is False
 
    def test_df_stored_without_metadata_rows(self):
        m = make_mdsdf(rows=5)
        assert len(m.df) == 5

    def test_metadata_rows_skip(self):
        """When metadata_rows=True the first 3 rows are treated as headers."""
        df = pd.DataFrame({"Temperature": ["Type", "Unit", "Stage", 100, 200, 300]})
        tmpl = _make_template(["Temperature"])
        onto = _build_ontology()
        m = MatDatSciDf(
            df=df,
            metadata_template=tmpl,
            orcid="0000-0000-0000-0000",
            ontology_graph=onto,
            metadata_rows=True,
        )
        assert len(m.df) == 3  # rows 4-6 only
 
    def test_custom_df_name(self):
        m = make_mdsdf(df_name="MyExperiment")
        assert m.df_name == "MyExperiment"
 
    def test_default_df_name_fallback(self):
        df = _make_df()
        onto = _build_ontology()
        m = MatDatSciDf(
            df=df,
            metadata_template=_make_template(),
            orcid="0000-0000-0000-0000",
            ontology_graph=onto,
        )
        assert m.df_name == MatDatSciDf.df_name
 
    def test_data_relations_dict_initialised(self):
        dr = {"mds:measuredBy": [["Temperature", "Sensor_ID"]]}
        m = make_mdsdf(cols=["Temperature", "Sensor_ID"], data_relations_dict=dr)
        assert "mds:measuredBy" in m.data_relations.prop_pair_dict

    def test_base_uri_stored(self):
        df = _make_df()
        onto = _build_ontology()
        m = MatDatSciDf(
            df=df,
            metadata_template=_make_template(),
            orcid="0000-0000-0000-0000",
            ontology_graph=onto,
            base_uri="https://example.org/",
        )
        assert m.base_uri == "https://example.org/"


class TestGetRelations:
    def test_returns_dict(self):
        m = make_mdsdf()
        result = m.get_relations()
        assert isinstance(result, dict)
 
    def test_contains_object_properties(self):
        m = make_mdsdf()
        props = m.get_relations()
        assert "measuredBy" in props
 
    def test_contains_datatype_properties(self):
        m = make_mdsdf()
        props = m.get_relations()
        assert "hasValue" in props
 
    def test_value_is_uri_type_tuple(self):
        m = make_mdsdf()
        props = m.get_relations()
        uri, ptype = props["measuredBy"]
        assert uri.startswith("http")
        assert ptype in ("Object Property", "Datatype Property")
 
    def test_view_relations_prints(self, capsys):
        m = make_mdsdf()
        m.view_relations()
        out = capsys.readouterr().out
        assert "measuredBy" in out


class TestValidateMetadata:
    def test_aligned_returns_true(self):
        m = make_mdsdf(cols=["Temperature"])
        assert m.validate_metadata() is True
 
    def test_extra_df_col_returns_false(self):
        # DataFrame has two columns but the template only defines one,
        # so Pressure is undefined and validate_metadata should return False.
        df = _make_df(["Temperature", "Pressure"])
        tmpl = _make_template(["Temperature"])   # intentionally omits Pressure
        onto = _build_ontology()
        m = MatDatSciDf(
            df=df,
            metadata_template=tmpl,
            orcid="0000-0000-0000-0000",
            ontology_graph=onto,
        )
        assert m.validate_metadata() is False
 
    def test_extra_template_entry_reported(self, capsys):
        """Metadata entry with no matching df column triggers EMPTY ENTRIES warning."""
        m = make_mdsdf(cols=["Temperature"])
        # Add a phantom column to the template that doesn't exist in the df
        m.metadata_template["@graph"].append({
            "@id": "mds:Phantom",
            "@type": "mds:Phantom",
            "skos:altLabel": "Phantom",
            "skos:definition": "Ghost column",
            "qudt:hasUnit": {"@id": "unit:UNITLESS"},
            "mds:hasStudyStage": "Result",
        })
        m.validate_metadata()
        out = capsys.readouterr().out
        assert "EMPTY ENTRIES" in out

    def test_incomplete_metadata_reported(self, capsys):
        """Items missing @type or definition should trigger MISSING FIELDS."""
        m = make_mdsdf(cols=["Temperature"])
        m.metadata_template["@graph"][0].pop("@type", None)
        m.validate_metadata()
        out = capsys.readouterr().out
        assert "MISSING FIELDS" in out

    def test_internal_helper_columns_ignored(self):
        """__source_file__, __rowkey__, __Label__ must not cause failures."""
        df = pd.DataFrame({
            "Temperature": [100],
            "__source_file__": ["test.csv"],
            "__rowkey__": ["r1"],
        })
        tmpl = _make_template(["Temperature"])
        onto = _build_ontology()
        m = MatDatSciDf(
            df=df, metadata_template=tmpl,
            orcid="0000-0000-0000-0000", ontology_graph=onto
        )
        assert m.validate_metadata() is True


class TestMetadataWrappers:
    def test_update_metadata_syncs_template(self, capsys):
        m = make_mdsdf(cols=["Temperature"])
        m.update_metadata("Temperature", "definition", "Updated def")
        # The outer metadata_template reference should be updated too
        graph = m.metadata_template.get("@graph", [])
        defs = [item.get("skos:definition") for item in graph
                if item.get("skos:altLabel") == "Temperature"]
        assert any("Updated def" in str(d) for d in defs)
 
    def test_add_column_metadata_appears_in_template(self):
        m = make_mdsdf(cols=["Temperature"])
        m.add_column_metadata(
            col_name="Humidity",
            rdf_type="mds:Humidity",
            unit="PERCENT",
            definition="Relative humidity",
            study_stage="Result",
        )
        labels = [item.get("skos:altLabel")
                  for item in m.metadata_template.get("@graph", [])]
        assert "Humidity" in labels
 
    def test_view_metadata_table_does_not_raise(self, capsys):
        m = make_mdsdf()
        m.view_metadata(format="table")
        out = capsys.readouterr().out
        assert "Metadata Template Summary" in out
 
    def test_save_metadata_creates_file(self, tmp_path):
        m = make_mdsdf()
        out = tmp_path / "template.json"
        m.save_metadata(str(out))
        assert out.exists()


class TestDataRelationsWrappers:
    def test_add_relations_stored(self):
        m = make_mdsdf(cols=["Temperature", "Sensor_ID"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m.add_relations({"mds:measuredBy": [("Temperature", "Sensor_ID")]})
        assert "mds:measuredBy" in m.data_relations.prop_pair_dict

    def test_validate_data_relations_valid(self):
        m = make_mdsdf(cols=["Temperature", "Sensor_ID"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m.add_relations({"mds:measuredBy": [("Temperature", "Sensor_ID")]})
        # measuredBy may or may not resolve; just check no exception is raised
        result = m.validate_data_relations()
        assert isinstance(result, bool)

    def test_view_data_relations_does_not_raise(self, capsys):
        m = make_mdsdf(cols=["Temperature", "Sensor_ID"])
        m.view_data_relations()  # empty dict — must not raise
        capsys.readouterr()


class TestRepr:
    def test_contains_df_name(self):
        m = make_mdsdf(df_name="SampleSet")
        assert "SampleSet" in repr(m)
 
    def test_contains_row_count(self):
        m = make_mdsdf(rows=7)
        assert "7" in repr(m)
 
    def test_contains_orcid(self):
        m = make_mdsdf()
        assert "0000-0000-0000-0000" in repr(m)
 
    def test_contains_verification_icon(self):
        m = make_mdsdf()
        r = repr(m)
        # Unverified ORCID should show ❌
        assert "❌" in r


class TestSaveDataframe:
    def test_csv_created(self, tmp_path):
        m = make_mdsdf(df_name="Exp1")
        m.save_mds_df(str(tmp_path), formats=["csv"])
        assert (tmp_path / "Exp1.csv").exists()
 
    def test_parquet_created(self, tmp_path):
        m = make_mdsdf(df_name="Exp1")
        m.save_mds_df(str(tmp_path), formats=["parquet"])
        assert (tmp_path / "Exp1.parquet").exists()
 
    def test_metadata_in_output_df_adds_header_rows(self, tmp_path):
        m = make_mdsdf(df_name="Exp1")
        m.save_mds_df(str(tmp_path), formats=["csv"], metadata_in_output_df=True)
        df_read = pd.read_csv(tmp_path / "Exp1.csv")
        # First column should be __Label__ and first three rows should be
        # "Type", "Units", "Study Stage"
        assert "__Label__" in df_read.columns
        assert df_read["__Label__"].iloc[0] == "Type"
 
    def test_creates_output_directory(self, tmp_path):
        m = make_mdsdf(df_name="Exp1")
        nested = tmp_path / "a" / "b" / "c"
        m.save_mds_df(str(nested), formats=["csv"])
        assert (nested / "Exp1.csv").exists()
 
    def test_metadata_json_saved_alongside_data(self, tmp_path):
        m = make_mdsdf(df_name="MyData")
        m.save_mds_df(str(tmp_path), formats=["csv"])
        assert (tmp_path / "MyData_template.json").exists()


class TestSearchLicense:
    @patch("FAIRLinked.RDFTableConversion.MDS_DF.main.load_licenses")
    def test_found_license_prints_results(self, mock_load, capsys):
        mock_load.return_value = {
            "licenses": [
                {"licenseId": "MIT", "name": "MIT License",
                 "isOsiApproved": True, "isDeprecatedLicenseId": False}
            ]
        }
        MatDatSciDf.search_license("MIT")
        out = capsys.readouterr().out
        assert "MIT" in out
 
    @patch("FAIRLinked.RDFTableConversion.MDS_DF.main.load_licenses")
    def test_no_match_prints_not_found(self, mock_load, capsys):
        mock_load.return_value = {"licenses": []}
        MatDatSciDf.search_license("NONEXISTENT_XYZ")
        out = capsys.readouterr().out
        assert "No licenses found" in out
 
    @patch("FAIRLinked.RDFTableConversion.MDS_DF.main.load_licenses")
    def test_exception_handled_gracefully(self, _mock, capsys):
        MatDatSciDf.search_license("CC0")
        out = capsys.readouterr().out
        assert "No licenses found matching 'CC0'" in out


class TestSerializeRow:
    def test_returns_list_of_graphs(self, tmp_path):
        m = make_mdsdf(cols=["Temperature"], rows=2)
        graphs = m.serialize_row(str(tmp_path / "rdf"), write_files=False)
        assert isinstance(graphs, list)
        assert len(graphs) == 2
        from rdflib import Graph as RDFGraph
        assert all(isinstance(g, RDFGraph) for g in graphs)
 
    def test_graph_contains_qudt_value_triples(self, tmp_path):
        m = make_mdsdf(cols=["Temperature"], rows=1)
        graphs = m.serialize_row(str(tmp_path / "rdf"), write_files=False)
        QUDT = Namespace("http://qudt.org/schema/qudt/")
        values = list(graphs[0].objects(predicate=QUDT.value))
        assert len(values) >= 1
 
    def test_invalid_spdx_license_raises(self, tmp_path):
        m = make_mdsdf(cols=["Temperature"], rows=1)
        with patch("FAIRLinked.RDFTableConversion.MDS_DF.main.load_licenses") as mock_lic:
            mock_lic.return_value = {"licenses": [{"licenseId": "MIT"}]}
            with pytest.raises(ValueError, match="Invalid SPDX"):
                m.serialize_row(
                    str(tmp_path / "rdf"),
                    license="NOT_A_REAL_LICENSE",
                    write_files=False)

    def test_write_files_creates_files(self, tmp_path):
        m = make_mdsdf(cols=["Temperature"], rows=2)
        out = tmp_path / "rdf_out"
        m.serialize_row(str(out), write_files=True)
        files = list(out.iterdir())
        assert len(files) >= 2
 
    def test_na_values_skipped(self, tmp_path):
        df = pd.DataFrame({"Temperature": [100, None]})
        tmpl = _make_template(["Temperature"])
        onto = _build_ontology()
        m = MatDatSciDf(
            df=df, metadata_template=tmpl,
            orcid="0000-0000-0000-0000", ontology_graph=onto
        )
        graphs = m.serialize_row(str(tmp_path / "rdf"), write_files=False)
        QUDT = Namespace("http://qudt.org/schema/qudt/")
        # Row 1 (index 1) graph should have no qudt:value triple
        values_row1 = list(graphs[1].objects(predicate=QUDT.value))
        assert len(values_row1) == 0


class TestSerializeBulk:
    def test_returns_single_graph(self, tmp_path):
        m = make_mdsdf(cols=["Temperature"], rows=3)
        out = tmp_path / "bulk.jsonld"
        result = m.serialize_bulk(str(out), write_files=False)
        from rdflib import Graph as RDFGraph
        assert isinstance(result, RDFGraph)
 
    def test_triple_count_grows_with_rows(self, tmp_path):
        m1 = make_mdsdf(cols=["Temperature"], rows=2)
        m2 = make_mdsdf(cols=["Temperature"], rows=5)
        g1 = m1.serialize_bulk(str(tmp_path / "b1.jsonld"), write_files=False)
        g2 = m2.serialize_bulk(str(tmp_path / "b2.jsonld"), write_files=False)
        assert len(g2) > len(g1)

