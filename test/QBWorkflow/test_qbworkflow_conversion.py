import os
import json
import pandas as pd
import pytest
import rdflib
from rdflib import Graph, Namespace, RDF, RDFS, OWL, URIRef, Literal
from rdflib.namespace import RDF, QB, DCTERMS
import sys
import FAIRLinked.RDFTableConversion
from FAIRLinked.RDFTableConversion import extract_data_from_csv, extract_from_folder
from unittest.mock import patch, MagicMock
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import json


import unittest
import tempfile
import shutil
from pathlib import Path

from FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper import prompt_for_missing_fields, normalize, jsonld_template_generator, extract_qudt_units


from FAIRLinked.QBWorkflow.rdf_transformer import convert_row_by_row, prepare_namespaces, convert_entire_dataset, convert_row_by_row_CRADLE




"""
 pytest suite for testing RDF transformer conversion methods.

Tests cover:
1. convert_row_by_row() - Each row becomes a separate RDF dataset
2. convert_row_by_row_CRADLE() - CRADLE-specific row-by-row conversion
3. convert_entire_dataset() - Entire DataFrame as single RDF dataset with slices

KEY FIX: Mocks the input_handler functions (get_approved_id_columns, get_row_identifier_columns)
to prevent tests from hanging while waiting for user input.

Run with: pytest test_rdf_transformer.py -v
"""


sys.path.insert(0, '/mnt/user-data/uploads')



# =============================================================================
#                           TEST FIXTURES
# =============================================================================

@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def namespace_map():
    """Load namespace mappings from the namespace template."""
    file_path = 'test/test_data/QB_test_data/namespace_template.xlsx'
    ns_df = pd.read_excel(file_path)
    
    ns_dict = {}
    for _, row in ns_df.iterrows():
        prefix = row['Namespace you are using']
        uri = row['Base URI']
        if pd.notna(prefix) and pd.notna(uri):
            ns_dict[prefix] = uri
    
    return ns_dict


@pytest.fixture
def simple_test_dataframe():
    """Create a simple test DataFrame for basic functionality tests."""
    return pd.DataFrame({
        'ExperimentId': ['EXP001', 'EXP002', 'EXP003'],
        'Temperature': [25.0, 30.0, 35.0],
        'Pressure': [101.3, 102.5, 103.1],
        'Material': ['Iron', 'Copper', 'Zinc'],
    })



@pytest.fixture
def xrd_dataframe():
    """Create a simple test DataFrame for basic functionality tests."""
    pd.read_excel('test/test_data/QB_test_data/xrd_data_demo.xlsx')



@pytest.fixture
def simple_metadata():
    """Create simple metadata for the simple test DataFrame."""
    return {
        'ExperimentId': {'IsMeasure': 'NO', 'Unit': None, 'Category': None, 'ExistingURI': None},
        'Temperature': {'IsMeasure': 'YES', 'Unit': 'qudt:DEG_C', 'Category': None, 'ExistingURI': None},
        'Pressure': {'IsMeasure': 'YES', 'Unit': 'qudt:KiloPA', 'Category': None, 'ExistingURI': None},
        'Material': {'IsMeasure': 'NO', 'Unit': None, 'Category': None, 'ExistingURI': None},
    }


@pytest.fixture
def mock_user_input():
    """
    CRITICAL FIXTURE: Mocks the input_handler functions to prevent hanging.
    
    The functions get_approved_id_columns and get_row_identifier_columns 
    normally prompt for user input. This fixture mocks them to return
    ['ExperimentId'] automatically.
    """


    with patch('FAIRLinked.QBWorkflow.rdf_transformer.get_approved_id_columns') as mock_approved, \
         patch('FAIRLinked.QBWorkflow.rdf_transformer.get_row_identifier_columns') as mock_row_ids:

 
        mock_approved.return_value = ['ExperimentId']
        mock_row_ids.return_value = ['ExperimentId']
        
        yield {'approved': mock_approved, 'row_ids': mock_row_ids}



# =============================================================================
#                    TEST: convert_row_by_row()
# =============================================================================

class TestConvertRowByRow:
    """Tests for the convert_row_by_row() method."""
    
    def test_creates_separate_files_per_row(self, simple_test_dataframe, simple_metadata, 
                                           namespace_map, temp_output_dir, mock_user_input):
        """Verify that each row produces separate TTL/JSON-LD/hash files."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        convert_row_by_row(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_output_dir,
            overall_timestamp='20250128120000'
        )
        
        # Verify subfolders
        assert os.path.exists(os.path.join(temp_output_dir, 'ttl'))
        assert os.path.exists(os.path.join(temp_output_dir, 'jsonld'))
        assert os.path.exists(os.path.join(temp_output_dir, 'hash'))
        
        # Count files
        ttl_files = list(Path(temp_output_dir, 'ttl').glob('*.ttl'))
        jsonld_files = list(Path(temp_output_dir, 'jsonld').glob('*.jsonld'))
        hash_files = list(Path(temp_output_dir, 'hash').glob('*.sha256'))
        
        assert len(ttl_files) == 3, "Should create 3 TTL files (one per row)"
        assert len(jsonld_files) == 3, "Should create 3 JSON-LD files"
        assert len(hash_files) == 3, "Should create 3 hash files"





    def test_generates_valid_rdf_graphs(self, simple_test_dataframe, simple_metadata, 
                                       namespace_map, temp_output_dir, mock_user_input):
        """Verify that generated RDF contains required Data Cube elements."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        convert_row_by_row(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_output_dir,
            overall_timestamp='20250128120000'
        )
        
        # Parse first TTL file
        ttl_files = list(Path(temp_output_dir, 'ttl').glob('*.ttl'))
        g = Graph()
        g.parse(str(ttl_files[0]), format='turtle')
        
        # Check for Data Cube components
        datasets = list(g.subjects(RDF.type, QB.DataSet))
        slices = list(g.subjects(RDF.type, QB.Slice))
        slice_keys = list(g.subjects(RDF.type, QB.SliceKey))
        
        assert len(datasets) > 0, "Graph should contain qb:DataSet"
        assert len(slices) > 0, "Graph should contain qb:Slice"
        assert len(slice_keys) > 0, "Graph should contain qb:SliceKey"


    def test_filenames_contain_experiment_ids(self, simple_test_dataframe, simple_metadata, 
                                             namespace_map, temp_output_dir, mock_user_input):
        """Verify filenames include ExperimentId values."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        convert_row_by_row(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_output_dir,
            overall_timestamp='20250128120000'
        )
        
        ttl_files = [f.name for f in Path(temp_output_dir, 'ttl').glob('*.ttl')]
        
        # Check for ExperimentId presence
        assert any('EXP001' in f for f in ttl_files), "EXP001 should be in filename"
        assert any('EXP002' in f for f in ttl_files), "EXP002 should be in filename"
        assert any('EXP003' in f for f in ttl_files), "EXP003 should be in filename"
 

    def test_hash_files_valid_sha256(self, simple_test_dataframe, simple_metadata, 
                                    namespace_map, temp_output_dir, mock_user_input):
        """Verify hash files contain valid SHA-256 hashes."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        convert_row_by_row(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_output_dir,
            overall_timestamp='20250128120000'
        )
        
        hash_files = list(Path(temp_output_dir, 'hash').glob('*.sha256'))
        
        for hash_file in hash_files:
            with open(hash_file, 'r') as f:
                hash_value = f.read().strip()
                assert len(hash_value) == 64, "SHA-256 hash should be 64 hex characters"
                assert all(c in '0123456789abcdef' for c in hash_value.lower()), \
                    "Hash should only contain hex characters"

"""
    def test_experimental_data(self, xrd_dataframe, simple_metadata, 
                                    namespace_map, temp_output_dir, mock_user_input):

        ns_map = prepare_namespaces(namespace_map, 'mds')
        

        convert_row_by_row(
            df=xrd_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_output_dir,
            overall_timestamp='20250128120000'
        )
        
        # Verify subfolders
        assert os.path.exists(os.path.join(temp_output_dir, 'ttl'))
        assert os.path.exists(os.path.join(temp_output_dir, 'jsonld'))
        assert os.path.exists(os.path.join(temp_output_dir, 'hash'))
        
        # Count files
        ttl_files = list(Path(temp_output_dir, 'ttl').glob('*.ttl'))
        jsonld_files = list(Path(temp_output_dir, 'jsonld').glob('*.jsonld'))
        hash_files = list(Path(temp_output_dir, 'hash').glob('*.sha256'))
        
        assert len(ttl_files) == 3, "Should create 3 TTL files (one per row)"
        assert len(jsonld_files) == 3, "Should create 3 JSON-LD files"
        assert len(hash_files) == 3, "Should create 3 hash files"


"""


# =============================================================================
#                    TEST: convert_row_by_row_CRADLE()
# =============================================================================

class TestConvertRowByRowCRADLE:
    """Tests for the convert_row_by_row_CRADLE() method."""
    
    def test_creates_files_with_random_letter_prefix(self, simple_test_dataframe, simple_metadata, 
                                                     namespace_map, temp_output_dir, mock_user_input):
        """Verify CRADLE method adds random letter prefix to filenames."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        convert_row_by_row_CRADLE(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_output_dir,
            overall_timestamp='20250128120000'
        )
        
        ttl_files = [f.name for f in Path(temp_output_dir, 'ttl').glob('*.ttl')]
        
        # Each file should start with lowercase letter + hyphen
        for filename in ttl_files:
            assert filename[0].islower() and filename[0].isalpha(), \
                f"File {filename} should start with lowercase letter"
            assert filename[1] == '-', \
                f"File {filename} should have hyphen after letter"
    
    def test_uses_hyphen_separators(self, simple_test_dataframe, simple_metadata, 
                                   namespace_map, temp_output_dir, mock_user_input):
        """Verify CRADLE uses hyphens instead of underscores in filenames."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        convert_row_by_row_CRADLE(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_output_dir,
            overall_timestamp='20250128120000'
        )
        
        ttl_files = [f.name for f in Path(temp_output_dir, 'ttl').glob('*.ttl')]
        
        for filename in ttl_files:
            base_name = filename.replace('.ttl', '')
            assert '-' in base_name, f"File {filename} should use hyphen separators"




# =============================================================================
#                    TEST: convert_entire_dataset()
# =============================================================================

class TestConvertEntireDataset:
    """Tests for the convert_entire_dataset() method."""
    
    def test_creates_single_file_output(self, simple_test_dataframe, simple_metadata, 
                                       namespace_map, temp_output_dir, mock_user_input):
        """Verify entire dataset mode creates only one set of files."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        output_folders = {
            'ttl': os.path.join(temp_output_dir, 'ttl'),
            'jsonld': os.path.join(temp_output_dir, 'jsonld'),
            'hash': os.path.join(temp_output_dir, 'hash')
        }
        for folder in output_folders.values():
            os.makedirs(folder, exist_ok=True)
        
        convert_entire_dataset(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            dataset_name='TestDataset',
            orcid='0000-0001-2345-6789',
            output_folder_paths=output_folders,
            overall_timestamp='20250128120000'
        )
        
        ttl_files = list(Path(output_folders['ttl']).glob('*.ttl'))
        jsonld_files = list(Path(output_folders['jsonld']).glob('*.jsonld'))
        hash_files = list(Path(output_folders['hash']).glob('*.sha256'))
        
        assert len(ttl_files) == 1, "Should create exactly 1 TTL file"
        assert len(jsonld_files) == 1, "Should create exactly 1 JSON-LD file"
        assert len(hash_files) == 1, "Should create exactly 1 hash file"
    
    def test_single_dataset_multiple_slices(self, simple_test_dataframe, simple_metadata, 
                                           namespace_map, temp_output_dir, mock_user_input):
        """Verify single dataset contains multiple slices (one per row)."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        output_folders = {
            'ttl': os.path.join(temp_output_dir, 'ttl'),
            'jsonld': os.path.join(temp_output_dir, 'jsonld'),
            'hash': os.path.join(temp_output_dir, 'hash')
        }
        for folder in output_folders.values():
            os.makedirs(folder, exist_ok=True)
        
        convert_entire_dataset(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            dataset_name='TestDataset',
            orcid='0000-0001-2345-6789',
            output_folder_paths=output_folders,
            overall_timestamp='20250128120000'
        )
        
        # Parse the graph
        ttl_file = list(Path(output_folders['ttl']).glob('*.ttl'))[0]
        g = Graph()
        g.parse(str(ttl_file), format='turtle')
        
        datasets = list(g.subjects(RDF.type, QB.DataSet))
        slices = list(g.subjects(RDF.type, QB.Slice))
        slice_keys = list(g.subjects(RDF.type, QB.SliceKey))
        
        assert len(datasets) == 1, "Should have exactly 1 DataSet"
        assert len(slices) == 3, "Should have 3 Slices (one per row)"
        assert len(slice_keys) == 1, "Should have 1 SliceKey"
    
    def test_dataset_has_metadata(self, simple_test_dataframe, simple_metadata, 
                                 namespace_map, temp_output_dir, mock_user_input):
        """Verify dataset has title and creator metadata."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        output_folders = {
            'ttl': os.path.join(temp_output_dir, 'ttl'),
            'jsonld': os.path.join(temp_output_dir, 'jsonld'),
            'hash': os.path.join(temp_output_dir, 'hash')
        }
        for folder in output_folders.values():
            os.makedirs(folder, exist_ok=True)
        
        test_orcid = '0000-0001-2345-6789'
        
        convert_entire_dataset(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            dataset_name='MyDataset',
            orcid=test_orcid,
            output_folder_paths=output_folders,
            overall_timestamp='20250128120000'
        )
        
        ttl_file = list(Path(output_folders['ttl']).glob('*.ttl'))[0]
        g = Graph()
        g.parse(str(ttl_file), format='turtle')
        
        dataset = list(g.subjects(RDF.type, QB.DataSet))[0]
        
        titles = list(g.objects(dataset, DCTERMS.title))
        creators = list(g.objects(dataset, DCTERMS.creator))
        
        assert len(titles) > 0, "Dataset should have a title"
        assert len(creators) > 0, "Dataset should have a creator"
        assert str(creators[0]) == test_orcid, "Creator should match provided ORCID"


# =============================================================================
#                    COMPARISON TESTS
# =============================================================================

class TestMethodComparisons:
    """Comparison tests between different conversion methods."""
    
    def test_row_by_row_produces_more_triples(self, simple_test_dataframe, simple_metadata, 
                                             namespace_map, mock_user_input):
        """Verify row-by-row produces more total triples than entire dataset."""
        ns_map = prepare_namespaces(namespace_map, 'mds')
        
        # Row-by-row conversion
        temp_rbr = tempfile.mkdtemp()
        convert_row_by_row(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            orcid='0000-0001-2345-6789',
            root_folder_path=temp_rbr,
            overall_timestamp='20250128120000'
        )
        
        rbr_triples = 0
        for ttl_file in Path(temp_rbr, 'ttl').glob('*.ttl'):
            g = Graph()
            g.parse(str(ttl_file), format='turtle')
            rbr_triples += len(g)
        
        # Entire dataset conversion
        temp_entire = tempfile.mkdtemp()
        output_folders = {
            'ttl': os.path.join(temp_entire, 'ttl'),
            'jsonld': os.path.join(temp_entire, 'jsonld'),
            'hash': os.path.join(temp_entire, 'hash')
        }
        for folder in output_folders.values():
            os.makedirs(folder, exist_ok=True)
        
        convert_entire_dataset(
            df=simple_test_dataframe,
            variable_metadata=simple_metadata,
            ns_map=ns_map,
            user_chosen_prefix='mds',
            dataset_name='TestDataset',
            orcid='0000-0001-2345-6789',
            output_folder_paths=output_folders,
            overall_timestamp='20250128120000'
        )
        
        ttl_file = list(Path(output_folders['ttl']).glob('*.ttl'))[0]
        g = Graph()
        g.parse(str(ttl_file), format='turtle')
        entire_triples = len(g)
        
        assert rbr_triples > entire_triples, \
            "Row-by-row should produce more triples due to duplicated structures"
        
        # Cleanup
        shutil.rmtree(temp_rbr)
        shutil.rmtree(temp_entire)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])



