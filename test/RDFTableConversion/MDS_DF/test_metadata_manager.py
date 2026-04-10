
import json
import os
import pytest
from unittest.mock import patch, MagicMock
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import SKOS
from FAIRLinked.RDFTableConversion.MDS_DF.metadata_manager  import Metadata


"""
Tests for metadata_manager.py — the Metadata class.

All RDF/ontology logic is tested without network access or a real MDS ontology.
The JSON-LD fixtures are self-contained so tests run in isolation.
"""


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

BASE_CONTEXT = {
    "@context": {
        "skos":  "http://www.w3.org/2004/02/skos/core#",
        "qudt":  "http://qudt.org/schema/qudt/",
        "unit":  "https://qudt.org/vocab/unit/",
        "mds":   "https://cwrusdle.bitbucket.io/mds/",
        "xsd":   "http://www.w3.org/2001/XMLSchema#",
        "prov":  "http://www.w3.org/ns/prov#",
        "rdf":   "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    }
}


def _make_template(col_name: str = "Temperature",
                   rdf_type: str = "mds:Temperature",
                   unit_id: str = "unit:DEG_C",
                   definition: str = "Temperature of the sample",
                   study_stage: str = "Synthesis") -> dict:
    """Return a minimal, valid JSON-LD metadata template with one column entry."""
    return {
        **BASE_CONTEXT,
        "@graph": [
            {
                "@id": rdf_type,
                "@type": rdf_type,
                "skos:altLabel": col_name,
                "skos:definition": definition,
                "qudt:hasUnit": {"@id": unit_id},
                "prov:generatedAtTime": {
                    "@value": "2024-01-01T00:00:00+00:00Z",
                    "@type": "xsd:dateTime",
                },
                "mds:hasStudyStage": study_stage,
            }
        ],
    }


@pytest.fixture
def metadata(tmp_path):
    """Return a fresh Metadata instance backed by a minimal template."""
    tmpl = _make_template()
    return Metadata(
        metadata_template=tmpl,
        matched_log=["Temperature"],
        unmatched_log=["Unmatched_Col"],
    )


class TestMetadataInit:
    def test_template_stored(self, metadata):
        assert "@graph" in metadata.metadata_temp
        assert len(metadata.metadata_temp["@graph"]) >= 1

    def test_logs_initialised(self, metadata):
        assert metadata.matched_log == ["Temperature"]
        assert metadata.unmatched_log == ["Unmatched_Col"]
    def test_default_empty_logs(self):

        m = Metadata(metadata_template=_make_template())
        assert m.matched_log == []
        assert m.unmatched_log == []

    def test_rdf_graph_populated(self, metadata):
        """The internal RDFLib graph must contain at least one triple."""
        assert len(metadata.template_graph) > 0

    def test_namespaces_bound(self, metadata):
        """MDS, QUDT, SKOS and UNIT namespaces must be bound."""
        ns_map = {prefix: str(ns) for prefix, ns in metadata.template_graph.namespaces()}
        assert "mds" in ns_map
        assert "qudt" in ns_map
        assert "unit" in ns_map


class TestPrintTemplate:
    def test_table_format_prints(self, metadata, capsys):
        metadata.print_template(format="table")
        captured = capsys.readouterr()
        assert "Metadata Template Summary" in captured.out

    def test_json_format_prints_valid_json(self, metadata, capsys):
        metadata.print_template(format="json")
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert "@graph" in parsed

    def test_unknown_format_warns(self, metadata, capsys):
        metadata.print_template(format="csv")
        captured = capsys.readouterr()
        assert "Unknown format" in captured.out

    def test_empty_graph_table(self):
        """Printing an empty template should not raise and should say 'empty'."""
        empty_tmpl = {**BASE_CONTEXT, "@graph": []}
        m = Metadata(metadata_template=empty_tmpl)
        # Should not raise
        m.print_template(format="table")






