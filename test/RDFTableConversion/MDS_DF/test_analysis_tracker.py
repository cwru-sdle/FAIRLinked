import json
import os
import warnings
import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch, call
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, OWL
from FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker import AnalysisTracker, AnalysisGroup

"""
Tests for analysis_tracker.py — AnalysisTracker and AnalysisGroup classes.

External dependencies (ORCID API, MDS ontology, psutil file handles) are mocked
so every test is fast and fully offline.
"""



# ---------------------------------------------------------------------------
# Shared helpers / stubs
# ---------------------------------------------------------------------------

MDS_NS = Namespace("https://cwrusdle.bitbucket.io/mds/")


def _build_ontology() -> Graph:
    g = Graph()
    g.bind("mds", MDS_NS)
    uri = MDS_NS["temperature"]
    g.add((uri, RDF.type, OWL.DatatypeProperty))
    g.add((uri, RDFS.label, Literal("temperature")))
    return g


# ── Patch expensive class-level loads ───────────────────────────────────────
@pytest.fixture(autouse=True)
def patch_mds_graph():
    onto = _build_ontology()
    with patch("FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker.MatDatSciDf.mds_graph", new=onto), \
         patch("FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker.load_mds_ontology_graph", return_value=onto):
        yield onto


# ── Silence ORCID API calls ──────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def patch_orcid():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    with patch("FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker.requests.get", return_value=mock_resp) as m:
        yield m


# ── Silence psutil (open file handles) ──────────────────────────────────────
@pytest.fixture(autouse=True)
def patch_psutil():
    with patch("FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker.psutil.Process") as mock_proc_cls:
        mock_proc_cls.return_value.open_files.return_value = []
        yield mock_proc_cls


# ── Silence ontology-matching helpers ───────────────────────────────────────
@pytest.fixture(autouse=True)
def patch_matchers():
    with patch("FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker.extract_terms_from_ontology", return_value=[]), \
         patch("FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker.find_best_match", return_value=None), \
         patch("FAIRLinked.RDFTableConversion.MDS_DF.analysis_tracker.get_curie", return_value="mds:unknown"):
        yield


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def make_tracker(proj_name="TestProj", home_path="/tmp/tracker_test",
                 orcid="0000-0000-0000-0000") -> "AnalysisTracker":
    onto = _build_ontology()
    return AnalysisTracker(
        proj_name=proj_name,
        home_path=home_path,
        orcid=orcid,
        ontology_graph=onto,
    )


# ===========================================================================
# AnalysisTracker.__init__
# ===========================================================================
#
class TestAnalysisTrackerInit:
    def test_placeholder_orcid_not_verified(self):
        t = make_tracker()
        assert t.orcid == "0000-0000-0000-0000"
        assert t.orcid_verified is False
#
    def test_real_orcid_verified_on_200(self, patch_orcid):
        patch_orcid.return_value.status_code = 200
        t = make_tracker(orcid="0000-0001-2345-6789")
        assert t.orcid_verified is True
#
    def test_real_orcid_unverified_on_404(self, patch_orcid):
        patch_orcid.return_value.status_code = 404
        with pytest.warns(UserWarning):
            t = make_tracker(orcid="0000-0001-2345-6789")
        assert t.orcid_verified is False
#
    def test_connection_error_marks_unverified(self, patch_orcid):
        import requests as req
        patch_orcid.side_effect = req.exceptions.ConnectionError("no net")
        with pytest.warns(UserWarning):
            t = make_tracker(orcid="0000-0001-2345-6789")
        assert t.orcid_verified is False
#
    def test_attributes_set(self):
        t = make_tracker(proj_name="MyProj", home_path="/data")
        assert t.proj_name == "MyProj"
        assert t.home_path == "/data"
        assert t.sources == []
        assert t.file_events == []
        assert t.analysis_id.startswith("run_")
#
#
## ===========================================================================
## get_context
## ===========================================================================
#
class TestGetContext:
    def test_returns_dict_with_required_keys(self):
        t = make_tracker()
        ctx = t.get_context()
        for key in ("qudt", "mds", "skos", "prov", "dcterms", "unit"):
            assert key in ctx, f"Missing context key: {key}"
#
    def test_prefix_key_present(self):
        onto = _build_ontology()
        t = AnalysisTracker(
            proj_name="P", home_path="/tmp",
            ontology_graph=onto, prefix="custom"
        )
        assert "custom" in t.get_context()
#
#
## ===========================================================================
## _route_data (internal dispatch)
## ===========================================================================
#
class TestRouteData:
    def test_routes_scalar_to_track_simple_datatype(self):
        t = make_tracker()
        t._route_data("alpha", 3.14)
        assert any(s["skos:altLabel"] == "alpha" for s in t.sources)
#
    def test_routes_dataframe(self):
        t = make_tracker()
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        t._route_data("my_df", df)
        entry = next(s for s in t.sources if s["skos:altLabel"] == "my_df")
        assert entry["mds:argumentType"] == "dataframe"
#
    def test_routes_dict_recursively(self):
        t = make_tracker()
        t._route_data("opts", {"key1": 1, "key2": 2.0})
        labels = [s["skos:altLabel"] for s in t.sources]
        assert "opts" in labels
        # Nested items should also be tracked
        assert "opts.key1" in labels or "opts.key2" in labels
#
    def test_routes_list(self):
        t = make_tracker()
        t._route_data("data_list", [1, 2, 3])
        entry = next(s for s in t.sources if s["skos:altLabel"] == "data_list")
        assert entry["mds:argumentType"] == "list"
#
    def test_routes_numpy_array(self):
        t = make_tracker()
        t._route_data("arr", np.array([1.0, 2.0, 3.0]))
        entry = next(s for s in t.sources if s["skos:altLabel"] == "arr")
        assert "mds:arrayShape" in entry
#
    def test_routes_unknown_type_to_track_other(self):
        t = make_tracker()
#
        class _Custom:
            pass
#
        t._route_data("obj", _Custom())
        assert any(s["skos:altLabel"] == "obj" for s in t.sources)
#
#
## ===========================================================================
## Individual tracking methods
## ===========================================================================
#
class TestTrackSimpleDatatype:
    def test_int_tracked(self):
        t = make_tracker()
        t.track_simple_datatype("count", 42)
        entry = next(s for s in t.sources if s["skos:altLabel"] == "count")
        assert entry["qudt:value"] == 42
#
    def test_str_tracked(self):
        t = make_tracker()
        t.track_simple_datatype("label", "hello")
        entry = next(s for s in t.sources if s["skos:altLabel"] == "label")
        assert entry["qudt:value"] == "hello"
#
    def test_bool_tracked(self):
        t = make_tracker()
        t.track_simple_datatype("flag", True)
        entry = next(s for s in t.sources if s["skos:altLabel"] == "flag")
        assert entry["qudt:value"] is True
#
    def test_parent_id_stored(self):
        t = make_tracker()
        t.track_simple_datatype("x", 1.0, parent_id="run_abc")
        entry = next(s for s in t.sources if s["skos:altLabel"] == "x")
        assert entry["mds:containerIdentifier"] == "run_abc"
#
#
#lass TestTrackDataframe:
    def test_columns_recorded(self):
        t = make_tracker()
        df = pd.DataFrame({"col_A": [1], "col_B": [2]})
        t.track_dataframe("df1", df)
        entry = next(s for s in t.sources if s["skos:altLabel"] == "df1")
        assert "col_A" in entry["mds:columnsList"]
        assert entry["mds:numberOfRows"] == 1
#
#
class TestTrackListArray:
    def test_1d_list_shape(self):
        t = make_tracker()
        t.track_list_array("my_list", [1, 2, 3, 4])
        entry = next(s for s in t.sources if s["skos:altLabel"] == "my_list")
        assert entry["mds:arrayShape"] == "4"
#
    def test_2d_numpy_shape(self):
        t = make_tracker()
        arr = np.zeros((3, 4))
        t.track_list_array("matrix", arr)
        entry = next(s for s in t.sources if s["skos:altLabel"] == "matrix")
        assert entry["mds:arrayShape"] == "3x4"
#
    def test_nested_list_shape(self):
        t = make_tracker()
        t.track_list_array("nested", [[1, 2], [3, 4], [5, 6]])
        entry = next(s for s in t.sources if s["skos:altLabel"] == "nested")
        assert "3" in entry["mds:arrayShape"]
#
#

class TestTrackOther:
    def test_unknown_object_tracked(self):
        t = make_tracker()
 
        class MyObj:
            pass
 
        t.track_other("obj", MyObj())
        assert any(s["skos:altLabel"] == "obj" for s in t.sources)
 
    def test_scalar_attributes_recursively_tracked(self):
        t = make_tracker()
 
        class Config:
            def __init__(self):
                self.lr = 0.01     
                self.epochs = 10  
 
        t.track_other("cfg", Config())
        labels = [s["skos:altLabel"] for s in t.sources]
        assert "cfg.lr" in labels
        assert "cfg.epochs" in labels



#
## ===========================================================================
## run_and_track
## ===========================================================================
#
class TestRunAndTrack:
    def test_function_executed_and_result_returned(self):
        t = make_tracker()
#
        def add(a, b):
            return a + b
#
        result = t.run_and_track(add, 2, 3)
        assert result == 5
#
    def test_arguments_captured(self):
        t = make_tracker()
#
        def double(x):
            return x * 2
#
        t.run_and_track(double, 7)
        labels = [s["skos:altLabel"] for s in t.sources]
        assert "x" in labels
#
    def test_tuple_outputs_both_tracked(self):
        t = make_tracker()
#
        def multi():
            return 1, 2
#
        t.run_and_track(multi)
        labels = [s["skos:altLabel"] for s in t.sources]
        assert any("output" in lbl for lbl in labels)
#
    def test_track_decorator(self):
        t = make_tracker()
#
        @t.track
        def compute(val):
            return val ** 2
#
        result = compute(4)
        assert result == 16
#
    def test_file_events_captured(self, patch_psutil):
        t = make_tracker()
        mock_file = MagicMock()
        mock_file.path = "/tmp/test_file.csv"
        mock_file.mode = "r"
        patch_psutil.return_value.open_files.return_value = [mock_file]
#
        def noop():
            pass
#
        t.run_and_track(noop)
        assert len(t.file_events) == 1
        assert t.file_events[0]["mds:fileName"] == "test_file.csv"
        assert t.file_events[0]["mds:fileEvent"] == "read/import"
#
#
## ===========================================================================
## create_analysis_jsonld
## ===========================================================================
#
class TestCreateAnalysisJsonld:
    def test_returns_valid_json_string(self):
        t = make_tracker()
        out = t.create_analysis_jsonld()
        data = json.loads(out)
        assert "@graph" in data
        assert "@context" in data
#
    def test_graph_contains_analysis_node(self):
        t = make_tracker()
        data = json.loads(t.create_analysis_jsonld())
        ids = [node.get("@id") for node in data["@graph"]]
        assert any(t.analysis_id in str(i) for i in ids)
#
    def test_orcid_embedded(self):
        t = make_tracker()
        data = json.loads(t.create_analysis_jsonld())
        raw = json.dumps(data)
        assert t.orcid in raw
#
    def test_sources_included(self):
        t = make_tracker()
        t.track_simple_datatype("x", 42)
        data = json.loads(t.create_analysis_jsonld())
        sources = data["@graph"][0].get("dcterms:source", [])
        assert len(sources) > 0
#
#
## ===========================================================================
## serialize_analysis_jsonld
## ===========================================================================
#
class TestSerializeAnalysisJsonld:
    def test_file_created(self, tmp_path):
        t = make_tracker(home_path=str(tmp_path))
        t.serialize_analysis_jsonld()
        json_dir = tmp_path / "analysis_json"
        files = list(json_dir.glob("*.json"))
        assert len(files) == 1
#
    def test_file_content_valid_json(self, tmp_path):
        t = make_tracker(proj_name="Proj", home_path=str(tmp_path))
        t.serialize_analysis_jsonld()
        json_dir = tmp_path / "analysis_json"
        content = json.loads(next(json_dir.glob("*.json")).read_text())
        assert "@graph" in content
#
#
## ===========================================================================
## create_report
## ===========================================================================
#
class TestCreateReport:
    def test_returns_string(self):
        t = make_tracker()
        r = t.create_report()
        assert isinstance(r, str)
#
    def test_contains_analysis_id(self):
        t = make_tracker()
        assert t.analysis_id in t.create_report()
#
    def test_contains_orcid(self):
        t = make_tracker()
        assert t.orcid in t.create_report()
#
    def test_no_sources_message(self):
        t = make_tracker()
        assert "No variables tracked" in t.create_report()
#
    def test_sources_listed(self):
        t = make_tracker()
        t.track_simple_datatype("learning_rate", 0.001)
        r = t.create_report()
        assert "learning_rate" in r
#
    def test_file_events_table_present(self, patch_psutil):
        t = make_tracker()
        mock_file = MagicMock()
        mock_file.path = "/data/input.csv"
        mock_file.mode = "r"
        patch_psutil.return_value.open_files.return_value = [mock_file]
#
        def noop():
            pass
#
        t.run_and_track(noop)
        r = t.create_report()
        assert "input.csv" in r
#
#
## ===========================================================================
## save_report
## ===========================================================================
#
class TestSaveReport:
    def test_report_file_created(self, tmp_path):
        t = make_tracker(proj_name="Exp", home_path=str(tmp_path))
        t.save_report()
        reports_dir = tmp_path / "reports"
        files = list(reports_dir.glob("*.md"))
        assert len(files) == 1
#
    def test_report_content_not_empty(self, tmp_path):
        t = make_tracker(home_path=str(tmp_path))
        t.save_report()
        md_file = next((tmp_path / "reports").glob("*.md"))
        assert len(md_file.read_text()) > 0
#
#
## ===========================================================================
## create_arg_df
## ===========================================================================
#
class TestCreateArgDf:
    def test_returns_dataframe(self):
        t = make_tracker()
        t.track_simple_datatype("x", 1)
        df = t.create_arg_df()
        assert isinstance(df, pd.DataFrame)
#
    def test_contains_rowkey_and_project_columns(self):
        t = make_tracker(proj_name="P1")
        t.track_simple_datatype("y", 2)
        df = t.create_arg_df()
        assert "__rowkey__" in df.columns
        assert "ProjectTitle" in df.columns
#
    def test_project_title_correct(self):
        t = make_tracker(proj_name="SuperProj")
        t.track_simple_datatype("z", 3)
        df = t.create_arg_df()
        assert df["ProjectTitle"].iloc[0] == "SuperProj"
#
#
## ===========================================================================
## AnalysisGroup
## ===========================================================================
#
class TestAnalysisGroup:
    def make_group(self, tmp_path) -> "AnalysisGroup":
        onto = _build_ontology()
        return AnalysisGroup(
            proj_name="GroupProj",
            home_path=str(tmp_path),
            ontology_graph=onto,
        )
#
    def test_init_empty(self, tmp_path):
        g = self.make_group(tmp_path)
        assert g.analyses == {}
        assert g.group_id.startswith("run_group_")
#
    def test_run_and_track_stores_analysis(self, tmp_path):
        g = self.make_group(tmp_path)
#
        def add(a, b):
            return a + b
#
        g.run_and_track(add, 1, 2)
        assert len(g.analyses) == 1
#
    def test_create_group_arg_df(self, tmp_path):
        g = self.make_group(tmp_path)
#
        def add(a, b):
            return a + b
#
        g.run_and_track(add, 1, 2)
        g.run_and_track(add, 3, 4)
        df = g.create_group_arg_df()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
#
    def test_create_group_arg_df_empty_warns(self, tmp_path):
        g = self.make_group(tmp_path)
        with pytest.warns(UserWarning):
            df = g.create_group_arg_df()
        assert df.empty
#
    def test_create_group_report_string(self, tmp_path):
        g = self.make_group(tmp_path)
#
        def add(a, b):
            return a + b
#
        g.run_and_track(add, 1, 2)
        report = g.create_group_report()
        assert isinstance(report, str)
        assert "GroupProj" in report
#
    def test_save_report_creates_md(self, tmp_path):
        g = self.make_group(tmp_path)
#
        def noop():
            pass
#
        g.run_and_track(noop)
        g.save_report()
        report_dir = tmp_path / g.group_id
        files = list(report_dir.glob("*.md"))
        assert len(files) == 1
#
    def test_save_jsonld_creates_json(self, tmp_path):
        g = self.make_group(tmp_path)
#
        def noop():
            pass
#
        g.run_and_track(noop)
        g.save_jsonld()
        json_dir = tmp_path / g.group_id / "_group_json"
        files = list(json_dir.glob("*.json"))
        assert len(files) == 1
#
    def test_get_context_has_required_keys(self, tmp_path):
        g = self.make_group(tmp_path)
        ctx = g.get_context()
        for key in ("qudt", "mds", "skos", "prov", "dcterms"):
            assert key in ctx
