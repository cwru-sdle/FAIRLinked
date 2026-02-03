

import pytest
import os
import tempfile
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import patch, MagicMock, mock_open
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF
import FAIRLinked.QBWorkflow.rdf_to_df as df_module  # Adjust import based on your module name


def test_thing():
    assert 1 ==1


class TestParseRdfToDf:
    """Test suite for parse_rdf_to_df function and its helpers"""

    @pytest.fixture
    def temp_files(self):
        """Create temporary files for testing"""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_json:
            json_path = tmp_json.name
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
            parquet_path = tmp_parquet.name
        
        yield json_path, parquet_path
        
        # Cleanup
        for path in [json_path, parquet_path]:
            if os.path.exists(path):
                os.remove(path)

    @pytest.fixture
    def sample_rdf_file(self):
        """Create a temporary sample RDF file"""
        with tempfile.NamedTemporaryFile(suffix='.ttl', delete=False, mode='w') as tmp:
            # Simple RDF data for testing
            tmp.write("""
                @prefix qb: <http://purl.org/linked-data/cube#> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                @prefix ex: <http://example.org#> .
                
                ex:dataset1 a qb:DataSet ;
                    qb:structure ex:dsd1 .
                    
                ex:dsd1 a qb:DataStructureDefinition ;
                    qb:component [ qb:dimension ex:ExperimentId ] .
            """)
            file_path = tmp.name
        yield file_path
        if os.path.exists(file_path):
            os.remove(file_path)

    def test_collect_rdf_files_single_file(self):
        """Test _collect_rdf_files with single file path"""
        with tempfile.NamedTemporaryFile(suffix='.ttl', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            result = df_module._collect_rdf_files(tmp_path)
            assert len(result) == 1
            assert result[0] == os.path.abspath(tmp_path)
        finally:
            os.remove(tmp_path)



    def test_collect_rdf_files_directory(self):
        """Test _collect_rdf_files with directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            ttl_file = os.path.join(tmpdir, "test.ttl")
            jsonld_file = os.path.join(tmpdir, "test.jsonld")
            other_file = os.path.join(tmpdir, "test.txt")
            
            for f in [ttl_file, jsonld_file, other_file]:
                with open(f, 'w') as fp:
                    fp.write("test")
            
            result = df_module._collect_rdf_files(tmpdir)
            result_files = [os.path.basename(f) for f in result]
            
            assert len(result) == 2
            assert "test.ttl" in result_files
            assert "test.jsonld" in result_files
            assert "test.txt" not in result_files




    def test_collect_rdf_files_no_valid_files(self):
        """Test _collect_rdf_files with no valid RDF files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create non-RDF file
            txt_file = os.path.join(tmpdir, "test.txt")
            with open(txt_file, 'w') as fp:
                fp.write("test")
            
            result = df_module._collect_rdf_files(tmpdir)
            assert result == []


