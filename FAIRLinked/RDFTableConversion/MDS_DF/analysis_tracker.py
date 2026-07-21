import json
import inspect
import pandas as pd
import numpy as np
from functools import wraps
from datetime import datetime
from uuid import uuid4
from typing import Optional, cast
import psutil
import os
from .main import MatDatSciDf
import sys
from rdflib import Graph, Namespace
from ...InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from .metadata_manager import Metadata
import warnings
import requests
from ... import __version__
from .utility import normalize_iri,load_licenses
from IPython.core.getipython import get_ipython
import types



##### ANALYSIS TRACKER ######

class AnalysisTracker:
    """
    A system for auditing scientific analysis, capturing data provenance, 
    and generating semantic JSON-LD metadata.
    """

    mds_graph = load_mds_ontology_graph()

    def __init__(self, 
                proj_name: str, 
                home_path: str, 
                orcid: Optional[str] = "0000-0000-0000-0000", 
                metadata_template: Optional[dict] = None,
                base_uri: Optional[str] = "https://cwrusdle.bitbucket.io/mds/",
                ontology_graph: Optional[Graph] = None, 
                prefix: Optional[str] = "mds",
                file_events: Optional[bool] = False) -> None:
        """
        Initializes the tracker with project metadata and researcher identity.

        Args:
            proj_name: Human-readable name of the research project.
            home_path: Root directory for storing all analysis artifacts.
            orcid: Researcher's ORCID iD for provenance attribution. 
                   Attempts to verify via Public API.
            metadata_template: Metadata information about analysis parameters.
            base_uri: The base URI for semantic namespace generation.
            ontology_graph: A custom RDFLib Graph. Defaults to MDS ontology.
            prefix: The prefix used for the base_uri in JSON-LD.
            file_events: Option to save file events. Default to False.
        """
        
        self.home_path = home_path
        self.file_events_store = file_events
        if orcid == "0000-0000-0000-0000" or orcid is None:
            self.orcid = "0000-0000-0000-0000"
            self.orcid_verified = False
            print("⚠️ Using Placeholder ORCID. This is not recommended for data publication.")
        else:
            try:
                clean_orcid = orcid.split("/")[-1].strip()
                response = requests.get(f"https://pub.orcid.org/v3.0/{clean_orcid}", 
                                        headers={'Accept': 'application/json'},
                                        timeout=5)
                
                if response.status_code == 200:
                    self.orcid = clean_orcid
                    self.orcid_verified = True
                else:
                    # Instead of crashing, we warn and mark as unverified
                    warnings.warn(f"❌ ORCID '{orcid}' not found. Analysis will be marked as UNVERIFIED.")
                    self.orcid = clean_orcid
                    self.orcid_verified = False
            
            except requests.exceptions.RequestException:
                warnings.warn("🌐 Connection Error: Could not verify ORCID. Tagging as UNVERIFIED.")
                self.orcid = orcid
                self.orcid_verified = False
        self.base_uri = base_uri
        self.prefix = prefix
        self.analysis_id = f"run{str(uuid4().int)[-15:].zfill(15)}"
        self.sources = []
        self.proj_name = proj_name
        self.file_events = []
        self.imports =[]
        self.activity_log = []
        self.ontology = ontology_graph
        if ontology_graph is None:
            if AnalysisTracker.mds_graph is None:
                print("""
                MDS-Onto from source is not available, please parse ontology from a local file.
                Run Analysis_instance.ontology.parse('path/to/ontology')
                """)
                user_defined_onto = Graph()
                self.ontology = user_defined_onto
            else:
                self.ontology = AnalysisTracker.mds_graph
        else:
            self.ontology = ontology_graph
        
        self.MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
        self.QUDT = Namespace("http://qudt.org/schema/qudt/")
        self.ontology.bind("mds", self.MDS)
        self.metadata_template = metadata_template if metadata_template else {}
        self.metadata_obj = Metadata(self.metadata_template)


    def get_context(self) -> dict:

        """
        Defines the JSON-LD context mapping prefixes to namespace URIs.

        Returns:
            dict: A dictionary of semantic prefix mappings (e.g., prov, mds, qudt).
        """

        return {
            self.prefix: self.base_uri,
            "qudt": "http://qudt.org/schema/qudt/",
            "mds": "https://cwrusdle.bitbucket.io/mds/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", 
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#", 
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "prov": "http://www.w3.org/ns/prov#",
            "dcterms": "http://purl.org/dc/terms/",
            "cco": "https://www.commoncoreontologies.org/",
            "unit": "https://qudt.org/vocab/unit/",
            "obo": "http://purl.obolibrary.org/obo/"
        }
    # --- IMPORT DETECTION ---

    @staticmethod
    def _get_module_info(module_name: str):
        """
        Pure utility to extract metadata from a loaded module.
        Type-safe for Pyright/Pylance.
        """
        
        root_name = module_name.split('.')[0]
        mod = sys.modules.get(root_name)
        
        info = {
            'skos:prefLabel': root_name,
            'dcterms:type': 'Software',
            'dcterms:hasVersion': 'Unknown',
            'dcterms:identifier': 'Unknown',
            'dcterms:publisher': 'Unknown'
        }

        # 1. Check Standard Library
        if root_name in sys.builtin_module_names:
            info['dcterms:publisher'] = 'Python Standard Library'
            if root_name == 'sys':
                info['dcterms:hasVersion'] = sys.version.split()[0]
        
        # 2. Check Filesystem Modules
        elif mod:
            # Safely get the file path to satisfy the type checker
            f_path = getattr(mod, '__file__', None)
            
            if f_path: # This check fixes the Pyright error
                info['dcterms:identifier'] = f_path
                info['dcterms:hasVersion'] = getattr(mod, '__version__', 
                                            getattr(mod, 'version', 'Unknown'))
                
                # Now Pyright knows f_path is a string
                if 'site-packages' in f_path or 'dist-packages' in f_path:
                    info['dcterms:publisher'] = 'Third Party Package'
                else:
                    # Use os.getcwd() to identify local project modules
                    if os.getcwd() in f_path:
                        info['dcterms:publisher'] = 'User Module'
                    else:
                        info['dcterms:publisher'] = 'System/Local Environment'
            else:
                # Handle modules that exist in memory but have no file (e.g., dynamically created)
                info['dcterms:publisher'] = 'In-Memory Module'
        
        return info

    @staticmethod
    def _categorize_imports(software_list: list):
        """
        Groups the flat software list into meaningful categories for the report.
        """
        categorized = {
            'third_party': [],
            'standard_library': [],
            'user_modules': [],
            'other': []
        }
        
        for sw in software_list:
            pub = sw.get('dcterms:publisher', 'Unknown')
            
            if pub == 'Third Party Package':
                categorized['third_party'].append(sw)
            elif pub == 'Python Standard Library':
                categorized['standard_library'].append(sw)
            elif pub == 'User Module':
                categorized['user_modules'].append(sw)
            else:
                categorized['other'].append(sw)
                
        return categorized

    def detect_all_imports(self):
        """
        Unified Environment Scanner for Jupyter and standard scripts.
        Identifies top-level software dependencies currently available in the session.
        """

        # 1. Access the current live namespace
        try:
            shell = get_ipython()
            # Use Jupyter's user namespace if available, otherwise fallback to globals
            namespace = shell.user_ns if shell else globals()
        except ImportError:
            namespace = globals()

        found_software = []
        seen_packages = set()

        # 2. Iterate through every object currently accessible to the user
        for name, obj in namespace.items():
            # Ignore private variables and internal Jupyter tools
            if name.startswith('_') or name == 'get_ipython':
                continue
                
            source_module = None
            
            # Check if the object is a module (import pandas as pd)
            if isinstance(obj, types.ModuleType):
                source_module = obj.__name__
            # Check if the object is an entity from a module (from pandas import DataFrame)
            elif hasattr(obj, '__module__'):
                source_module = getattr(obj, '__module__')

            if source_module:
                # Extract the root package name (e.g., 'pandas' from 'pandas.core.frame')
                root_pkg = source_module.split('.')[0]
                
                # 3. Only log unique, valid packages present in sys.modules
                if root_pkg in sys.modules and root_pkg not in seen_packages:
                    # Skip the tracker itself and standard built-ins to reduce noise
                    if root_pkg == self.__class__.__module__ or root_pkg == 'builtins':
                        continue
                    
                    # Fetch metadata using the static utility method
                    software_info = self._get_module_info(root_pkg)
                    
                    # Assign a stable FAIR identifier
                    software_info['@id'] = f'{self.prefix}:Software_{root_pkg}'
                    
                    found_software.append(software_info)
                    seen_packages.add(root_pkg)

        self.imports = found_software

    
    

    # --- WRAPPERS ---

    def track(self, func):
        """
        A decorator to automatically wrap a function with provenance tracking.

        Args:
            func: The function to be decorated.

        Returns:
            function: The wrapped function that executes via run_and_track.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.run_and_track(func, *args, **kwargs)
        return wrapper

    def run_and_track(self, func, *args, **kwargs):
        """
        Executes a function while auditing arguments, results, and environment.

        This method acts as a high-level provenance wrapper. It captures the 
        "top-most" (direct) input IRIs from the function signature and the 
        direct output IRIs from the return value. While all internal data 
        structures (like nested dictionary keys) are routed and saved to the 
        global metadata log, only the direct IRIs are linked to the Activity 
        node via CCO and PROV-O properties.

        The method performs the following audit steps:
            1. Generates a unique 15-digit numeric activity ID.
            2. Binds and routes direct function arguments to capture input IRIs.
            3. Triggers a live environment scan (imports/sys.modules).
            4. Executes the function while monitoring OS-level file handles.
            5. Routes and captures return value IRIs.
            6. Finalizes a Linked Data Activity node with prov:used and prov:generated.

        Args:
            func (callable): The scientific function or method to be executed.
            *args: Positional arguments to be passed to the target function.
            **kwargs: Keyword arguments to be passed to the target function.

        Returns:
            Any: The original return value of the wrapped function. If an 
                exception occurs, it returns None after logging the error 
                as a provenance event.
        """
        # 1. Setup Activity Identity
        activity_num = str(uuid4().int)[-15:]
        run_id = f"{func.__name__}_activity{activity_num}_{self.analysis_id}"
        activity_iri = f"{self.prefix}:{run_id}"
        start_time = datetime.now().isoformat()
        
        # Trackers for direct IRIs only
        direct_input_iris = []
        direct_output_iris = []

        # 2. Capture Direct Input IRIs from Signature
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        for name, val in bound_args.arguments.items():
            if name == 'self':
                continue
            
            # Capture the IRI string returned by the routing logic
            iri = self._route_data(name, val, parent_id=run_id)
            if iri:
                direct_input_iris.append(iri)
        
        # 3. Environment Audit
        self.detect_all_imports()
        process = psutil.Process(os.getpid())
        
        try:
            # Execute
            result = func(*args, **kwargs)
            end_time = datetime.now().isoformat()

            # 4. Capture Direct Output IRIs
            if isinstance(result, tuple):
                for i, item in enumerate(result):
                    out_iri = self._route_data(f"{func.__name__}_output_{i}", item, parent_id=run_id)
                    if out_iri: 
                        direct_output_iris.append(out_iri)
            else:
                out_iri = self._route_data(f"{func.__name__}_output", result, parent_id=run_id)
                if out_iri: 
                    direct_output_iris.append(out_iri)
            


            # 5. Finalize Activity with Direct Links
            self.activity_log.append({
                "@id": activity_iri,
                "@type": "cco:ont00000366", # Act of Information Processing
                "rdfs:label": "Act of Information Processing",
                "skos:altLabel": f"Execution of function {func.__name__}",
                "prov:startedAtTime": start_time,
                "prov:endedAtTime": end_time,
                "cco:ont00001921": direct_input_iris,      # Direct IRIs list
                "cco:ont00001986": direct_output_iris  # Direct IRIs list
            })
            
            # 6. Capture File Events linked to this Activity
            if self.file_events_store:
                for file in process.open_files():
                    mode = getattr(file, 'mode', 'r')
                    event_type = "read/import" if 'r' in mode else "write/modification"
                    self.file_events.append({
                            "@id": f"{self.prefix}:fileEvent{str(uuid4().int)[-15:]}_{self.analysis_id}",
                            "@type": "cco:ont00000958",
                            "mds:fileName": os.path.basename(file.path),
                            "mds:fileLocation": file.path,
                            "mds:fileEvent": event_type,
                            "prov:wasInformedBy": activity_iri,
                            "prov:generatedAtTime": datetime.now().isoformat()
                        })

            metadata_template, matched_log, unmatched_log = self.create_metadata_template()
            self.metadata_obj.update_bulk(metadata_template)
            self.semantic_remapping(unmatched_log)

            return result

        except Exception as e:
            error_msg = f"Error in {func.__name__}: {str(e)}"
            print(f"⚠️ {error_msg}")
            err_iri = self._route_data(f"{func.__name__}_ERROR", error_msg, parent_id=run_id)
            self.activity_log.append({
                "@id": activity_iri,
                "prov:startedAtTime": start_time,
                "cco:ont00001921": direct_input_iris,
                "cco:ont00001986": [err_iri] if err_iri else []
            })
            return None

    def run_and_track_R(self, r_func_name, *args, **kwargs):
        """
        Intercepts R function execution via reticulate, running the code 
        through the Python tracking pipeline before appending captured 
        R package metadata to the JSON-LD context.
        """
        # 1. Safely grab the R bridge object from the execution environment
        try:
            import __main__
            r = getattr(__main__, 'r')
        except AttributeError:
            raise RuntimeError(
                "R environment wrapper 'r' not found in __main__. "
                "Ensure reticulate has initialized the R session."
            )

        # 2. Setup the target R function wrapper
        r_target_function = getattr(r, r_func_name)
        
        def universal_bridge(*f_args, **f_kwargs):
            return r_target_function(*f_args, **f_kwargs)
            
        universal_bridge.__name__ = r_func_name
        
        # 3. Use the class's own native tracker pipeline
        result = self.run_and_track(universal_bridge, *args, **kwargs)
        
        # 4. Ask R for currently attached packages
        try:
            attached_packages = list(r.search())
            r_packages = [pkg.split(':')[1] for pkg in attached_packages if pkg.startswith('package:')]
        except Exception as e:
            print(f"Could not retrieve attached R packages: {e}")
            return result
        
        # 5. Ensure the imports array exists
        if self.imports is None:
            self.imports = []
            
        already_logged = {item.get('@id') for item in self.imports if isinstance(item, dict)}

        # 6. Shape and safely append R package metadata
        for pkg in r_packages:
            r_id = f'{self.prefix}:Software_{pkg}'
            
            if r_id not in already_logged:
                # Resolve version string
                try:
                    raw_version = r.packageVersion(pkg)
                    raw_str = str(r.format(raw_version))
                    r_version = raw_str.replace(', ', '.').replace(',', '.')
                except Exception as e:
                    print(f'Could not get version for {pkg}: {e}')
                    r_version = 'Unknown'
                    
                # Resolve file system path location
                try:
                    find_package_func = getattr(r, 'find.package')
                    r_location = str(find_package_func(pkg))
                except Exception as e:
                    print(f'Could not get installation location for {pkg}: {e}')
                    r_location = f'R_package:{pkg}'
                    
                r_software_info = {
                    '@id': r_id,
                    'skos:prefLabel': f'{pkg} (R Package)',
                    'dcterms:type': 'Software',
                    'dcterms:hasVersion': r_version,
                    'dcterms:identifier': r_location,
                    'dcterms:publisher': 'Third Party Package'
                }
                
                self.imports.append(r_software_info)

        # 7. Return the final execution result
        return result
        
    def semantic_remapping(self, unmatched_log):
            """
            Refines simple Python types by matching them against the 
            current metadata template's semantic types.
            """
            # 1. Pre-process the template into a quick-lookup dictionary
            # This prevents nested loops and significantly speeds up the process
            metadata_template = self.metadata_obj.metadata_temp
            graph_template = metadata_template.get("@graph", [])
            
            # Create a map of {altLabel: semantic_type}
            ontology_map = {
                item.get('skos:altLabel'): item.get('@type') 
                for item in graph_template 
                if item.get('skos:altLabel')
            }

            updated_sources = []
            
            for entry in self.sources:
                arg_type = entry.get("mds:argumentType")
                var_name = entry.get("skos:altLabel")

                # Check if it's a simple type and we have a semantic match
                if arg_type in ('int', 'float', 'str', 'bool'):
                    if var_name in unmatched_log:
                        semantic_type = "cco:ont00000958"
                    else:
                        semantic_type = ontology_map.get(var_name)
                    
                    if semantic_type:
                        # Upgrade from generic cco:ont00000958 to ontology-backed terms if matched
                        entry['@type'] = semantic_type
                
                # Always append the entry so we don't lose provenance data
                updated_sources.append(entry)

            self.sources = updated_sources
            print(f"✅ Semantic remapping complete. Checked {len(self.sources)} entries.")


    def _route_data(self, name, val, parent_id=None):
        """
        The central dispatcher that directs data to specific tracking methods 
        based on the object's type (DataFrame, Dict, List, etc.).

        Args:
            name: The variable name or identifier.
            val: The data object to be tracked.
            parent_id: Optional identifier of the parent container for nesting.
        """

        name = normalize_iri(name)
        if isinstance(val, pd.DataFrame):
            return self.track_dataframe(name, val, parent_id)
        elif isinstance(val, dict):
            return self.track_dict(name, val, parent_id)
        elif isinstance(val, (list, np.ndarray)):
            return self.track_list_array(name, val, parent_id)
        elif isinstance(val, (str, int, float, bool)):
            return self.track_simple_datatype(name, val, parent_id)
        else:
            return self.track_other(name, val, parent_id)


    # --- TRACKING METHODS ---

    def track_simple_datatype(self, name, val, parent_id=None):
        """
        Tracks primitive types (str, int, float, bool) and attempts to 
        map them to ontology terms using fuzzy matching.

        Args:
            name: Variable name.
            val: The primitive value.
            parent_id: ID of the containing process or object.
        """



        self.sources.append({
            "@id": f"{self.prefix}:{name}.{self.analysis_id}",
            "@type": "cco:ont00000958",
            "mds:argumentIdentifier": f"{name}.{self.analysis_id}",
            "skos:altLabel": name,
            "mds:argumentType": type(val).__name__,
            "qudt:value": val,
            "mds:containerIdentifier": {
                "@id": f"{self.prefix}:{parent_id}"
            }
        })

        return f"{self.prefix}:{name}.{self.analysis_id}"

    def track_dict(self, name, val, parent_id=None):
        """
        Logs a dictionary's keys and recursively tracks its nested values.

        Args:
            name: Dictionary name.
            val: The dictionary object.
            parent_id: ID of the containing process or object.
        """

        current_id = f"{name}.{self.analysis_id}"

        self.sources.append({
            "@id": f"{self.prefix}:{current_id}",
            "@type": "cco:ont00000958",
            "mds:argumentIdentifier": current_id,
            "skos:altLabel": name, 
            "mds:argumentType": "dictionary", 
            "mds:keys": list(val.keys()),
            "mds:containerIdentifier": {
                "@id": f"{self.prefix}:{parent_id}"
            } # Links to its container
        })
        for k, v in val.items():
            # Recursively pass the current dict as the new parent
            self._route_data(f"{name}/{k}", v, parent_id=current_id)
        
        return f"{self.prefix}:{current_id}"

    def track_dataframe(self, name, df, parent_id=None):
        """
        Logs structural metadata of a Pandas DataFrame, including column 
        names and row counts.

        Args:
            name: DataFrame name.
            df: The pandas DataFrame object.
            parent_id: ID of the containing process or object.
        """
        self.sources.append({
            "@id": f"{self.prefix}:{name}.{self.analysis_id}",
            "@type": "cco:ont00000958",
            "mds:argumentIdentifier": f"{name}.{self.analysis_id}",
            "skos:altLabel": name, 
            "mds:argumentType": "dataframe", 
            "mds:columnsList": list(df.columns), 
            "mds:numberOfRows": len(df),
            "mds:containerIdentifier": {
                "@id": f"{self.prefix}:{parent_id}"
            }
        })

        return f"{self.prefix}:{name}.{self.analysis_id}"

    def track_list_array(self, name, data, parent_id = None):
        """
        Tracks the dimensions and size of lists and NumPy arrays.

        Args:
            name: Array or list name.
            data: The sequence or array-like object.
            parent_id: ID of the containing process or object.
        """
        # 1. Handle NumPy Arrays
        if hasattr(data, 'shape'):
            dimensions = list(data.shape)
        
        # 2. Handle Nested Lists (The recursive way)
        elif isinstance(data, list):
            dimensions = []
            temp = data
            while isinstance(temp, list) and len(temp) > 0:
                dimensions.append(len(temp))
                temp = temp[0]
        
        else:
            dimensions = [len(data)]

        shape_str = "x".join(map(str, dimensions))

        self.sources.append({
            "@id": f"{self.prefix}:{name}.{self.analysis_id}",
            "@type": "cco:ont00000958",
            "mds:argumentIdentifier": f"{name}.{self.analysis_id}",
            "skos:altLabel": name,
            "mds:argumentType": type(data).__name__,
            "mds:listSize": len(data),
            "mds:arrayShape": shape_str,
            "mds:containerIdentifier": {
                "@id": f"{self.prefix}:{parent_id}"
            }
        })

        return f"{self.prefix}:{name}.{self.analysis_id}"

    def track_other(self, name, obj, parent_id=None):
        """
        Falls back to inspecting custom objects by logging their public 
        attributes as nested data.

        Args:
            name: Object name.
            obj: The Python object to inspect.
            parent_id: ID of the containing process or object.
        """
        current_id = f"{name}.{self.analysis_id}"
        
        # Log the object...
        self.sources.append({
            "@id": f"{self.prefix}:{current_id}",
            "@type": "cco:ont00000958",
            "mds:argumentIdentifier": current_id,
            "skos:altLabel": name, 
            "mds:argumentType": type(obj).__name__, 
            "mds:containerIdentifier": {
                "@id": f"{self.prefix}:{parent_id}"
            }
        })

        # Inspect the object...
        try:
            for attr_name, attr_val in vars(obj).items():
                if not attr_name.startswith('_') and (isinstance(attr_val, (int, float, str, bool, dict, list, pd.DataFrame) or hasattr(attr_val, '__dict__'))):
                    self._route_data(f"{name}/{attr_name}", attr_val, parent_id=current_id)
        except TypeError:
            pass

        return f"{self.prefix}:{current_id}"

    #### METADATA OBJECT WRAPPERS ####
    def update_metadata_bulk(self, metadata_template: dict):
        """
        Wrapper to update metadata template in bulk for multiple columns
        """
        self.metadata_obj.update_bulk(metadata_template=metadata_template)
        self.metdata_template = self.metadata_obj.metadata_temp

    def update_metadata(self, col_name: str, field: str, value: str):
        """
        Wrapper to update a metadata property (unit, type, definition, etc.) 
        for a specific column.
        """
        self.metadata_obj.update_template(col_name, field, value)
        self.metadata_template = self.metadata_obj.metadata_temp

    def add_column_metadata(self, col_name: str, rdf_type: str, unit: str = "UNITLESS", 
                            definition: str = "No definition provided", study_stage: str = "UNK"):
        """
        Top-level API to manually define semantic metadata for a new column.
        Useful for defining columns found in 'Discovery Warning' reports.
        """
        existing = self.metadata_obj.add_column_metadata(col_name, rdf_type, unit, definition, study_stage)
        if existing:
            print(f"⚠️ Metadata for '{col_name}' already exists. Use update_metadata instead.")
        self.metadata_template = self.metadata_obj.metadata_temp

    def overwrite_metadata(self, metadata_template: dict):
        """
        Wrapper to delete and replace metadata information. WARNING: THIS WILL DELETE ALL CURRENT METADATA
        """
        new_metadata_obj = Metadata(metadata_template=metadata_template)
        self.metadata_obj = new_metadata_obj
        self.metadata_template = metadata_template

    def delete_column_metadata(self, col_name: str):
        """
        Top-level API to remove a column's semantic metadata definition.
        Useful for cleaning up incorrect mappings or unwanted discovery columns.

        Args:
            col_name (str): The column label to remove from the metadata template.
        """
        self.metadata_obj.delete_column_metadata(col_name)
        self.metadata_template = self.metadata_obj.metadata_temp

    def view_metadata(self, format: str = "table"):
        """
        Wrapper to print the current metadata template as a 
        formatted table or raw JSON-LD. Change to format = 'json-ld' 
        to view metadata template in JSON-LD format.
        """
        self.metadata_obj.print_template(format=format)

    def save_metadata(self, output_path: str, matched_log_path: Optional[str] = None, 
                           unmatched_log_path: Optional[str] = None):
        """
        Wrapper to export the JSON-LD template and the status logs 
        (matched/unmatched columns) to files.
        """
        self.metadata_obj.save_metadata(output_path, matched_log_path, unmatched_log_path)


    def create_analysis_jsonld(self, license: Optional[str] = None):
        """
        Assembles all tracked data and file events into a valid JSON-LD string.

        Returns:
            str: A formatted JSON-LD string containing the analysis graph.
        """

        orcid_verification = "ORCID iD verified." if self.orcid_verified else "ORCID iD not verified."
        if(not license):
            license_uri = "https://spdx.org/licenses/CC0-1.0.html"
            print("No license provided. Default to CC0-1.0 (Public Domain)")

        elif not license.startswith("http"):
            # Load SPDX license list

            spdx_data = load_licenses()

            valid_ids = {lic["licenseId"] for lic in spdx_data["licenses"]}

            # Check if the provided short ID is valid
            if license not in valid_ids:
                raise ValueError(
                    f"Invalid SPDX license ID '{license}'.\n"
                    f"Please use one from https://spdx.org/licenses/."
                )

            license_uri = f"https://spdx.org/licenses/{license}.html"

        else:
            # Full URI provided; assume it's valid
            license_uri = license

        output = {
            "@context": self.get_context(),
            "@graph":[
            {
            "@id": f"mds:{self.analysis_id}",
            "@type": "mds:AnalyticalResult",
            "dcterms:creator":{
                "@id": f"https://orcid.org/{self.orcid}"
            },
            "dcterms:date": datetime.now().strftime("%Y-%m-%d"),
            "dcterms:source": self.sources,
            "dcterms:provenance": self.file_events,
            "dcterms:description": orcid_verification,
            "mds:hasStudyStage": "Analysis",
            "dcterms:requires": self.imports if self.imports else [],
            "obo:BFO_0000117": self.activity_log,
            "dcterms:license": {"@id": license_uri}
            },

            ]
    
        }

        return json.dumps(output, indent=2)

    def serialize_analysis_jsonld(self, license: Optional[str] = None):
        """
        Writes the JSON-LD metadata to a physical file within the analysis directory.
        """
        # 1. Define and create the directory
        json_dir = os.path.join(self.home_path, "analysis_json")
        os.makedirs(json_dir, exist_ok=True)

        # 2. Construct the specific filename
        filename = f"{self.proj_name}_{self.analysis_id}.json"
        full_path = os.path.join(json_dir, filename)

        # 3. Get the JSON-LD data and write to disk
        jsonld_data = self.create_analysis_jsonld(license=license)
        
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(jsonld_data)

        print(f"JSON-LD saved at {full_path}")

    def create_report(self) -> str:
        """
        Generates a human-readable Markdown summary of the analysis 
        variables and file system activities.

        Returns:
            str: A Markdown formatted report.
        """
        report = []
        report.append(f"## Analysis Report: {self.proj_name}")
        report.append(f"**Analysis ID:** `{self.analysis_id}`")
        report.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"**Creator:** [{self.orcid}](https://orcid.org/{self.orcid}) ({'Verified' if self.orcid_verified else 'Unverified'})")
        report.append("\n")

        # 1. Inputs and Outputs
        report.append("### 🧪 Data Sources & Variables")
        if not self.sources:
            report.append("_No variables tracked._")
        else:
            for s in self.sources:
                shape_info = f" (Shape: {s.get('mds:arrayShape')})" if 'mds:arrayShape' in s else ""
                val_info = f" = `{s.get('qudt:value')}`" if 'qudt:value' in s else ""
                report.append(f"* **{s['skos:altLabel']}** ({s['mds:argumentType']}){shape_info}{val_info}")

        # 2. Activity report
        report.append('\n### 🕹️ Activity Report')
        if not self.activity_log:
            report.append('_No activity tracked_')
        else:
            for act in self.activity_log:
                activity_info = f"{act.get('skos:altLabel')}"
                start = f"{act.get('prov:startedAtTime')}"
                end = f"{act.get('prov:endedAtTime')}"
                report.append(f"* **{activity_info}**; Started at time **{start}**; Ended at time **{end}**; Performed by **{self.orcid}**")


        # 3. System Imports
        report.append("\n### 📂 Software Environment")
        if not self.imports:
            report.append("_No software environment tracked._")
        else:
            categorized = self._categorize_imports(self.imports)
            
            sections = [
                ('third_party', '#### THIRD PARTY PACKAGES'),
                ('standard_library', '#### STANDARD LIBRARY'),
                ('user_modules', '#### USER/PROJECT MODULES'),
                ('other', '#### OTHER MODULES')
            ]

            for cat_key, title in sections:
                if not categorized.get(cat_key):
                    continue
                    
                report.append(f"\n{title}")
                for sw in categorized[cat_key]:
                    # Extract data directly from the software node
                    name = sw.get('skos:prefLabel')
                    version = sw.get('dcterms:hasVersion')
                    path = sw.get('dcterms:identifier')
                    alias = sw.get('skos:altLabel')
                    
                    # Formatting the summary line
                    alias_info = f" (accessed as '{alias}')" if alias else ""
                    report.append(f"  • **{name}**{alias_info}")

                    # Meta info
                    if version and version != "Unknown":
                        report.append(f"    └─ Version: {version}")
                    if path and path != "Unknown" and path != "Built-in":
                        report.append(f"    └─ Location: {path}")
        

        # 4. File System Events (The "Paper Trail")
        report.append("\n### 📂 File System Activity")
        if not self.file_events:
            report.append("_No file system events detected._")
        else:
            report.append("| File Name | Event | mds:fileLocation |")
            report.append("| :--- | :--- | :--- |")
            for e in self.file_events:
                report.append(f"| {e['mds:fileName']} | {e['mds:fileEvent']} | `{e['mds:fileLocation']}` |")


        report.append("\n---")
        
        return "\n".join(report)

    def save_report(self):
        """
        Saves the human-readable Markdown report to the reports directory.
        """
        report_dir = os.path.join(self.home_path, "reports")
        os.makedirs(report_dir, exist_ok=True)
        
        filename = f"{self.proj_name}_{self.analysis_id}_summary.md"
        full_path = os.path.join(report_dir, filename)
        
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(self.create_report())

        print(f"Report saved at {full_path}")

    def create_arg_df(self):
        """
        Flattens the tracked variables into a single-row Pandas DataFrame for 
        tabular comparison across different runs.

        Returns:
            pd.DataFrame: A DataFrame row containing run metadata and values.
        """
        row_data = {
            s["skos:altLabel"]: (s["qudt:value"] if s.get("mds:argumentType") in ['int', 'str', 'float', 'bool'] else s["mds:argumentIdentifier"]) 
            for s in self.sources
        }
    
        # 2. Add your mandatory tracking columns
        row_data["__rowkey__"] = self.analysis_id
        row_data["ProjectTitle"] = self.proj_name
    
        # 3. Create the DataFrame from this single-row dictionary
        # Wrapping row_data in a list [] tells Pandas this is one row of data
        return pd.DataFrame([row_data])

    def create_metadata_template(self):
        """
        Automatically generates a metadata template by matching 
        group data columns against the loaded ontology.

        Returns:
            tuple: (metadata_template, matched_log, unmatched_log)
        """
        arg_df = self.create_arg_df()
        ontology = self.ontology

        dummy_mdsdf = MatDatSciDf(df=arg_df, ontology_graph=ontology)
        metadata_template, matched_log, unmatched_log = dummy_mdsdf.template_generator(skip_prompts=True)

        return metadata_template, matched_log, unmatched_log

    



class AnalysisGroup:

    """
    Manages a collection of related AnalysisTracker instances, facilitating 
    group-level reporting and master graph generation.
    """
    mds_graph = load_mds_ontology_graph()

    def __init__(self,
                proj_name: str, 
                home_path: str, 
                orcid: Optional[str] = "0000-0000-0000-0000", 
                metadata_template: Optional[dict] = None,
                base_uri: Optional[str] = "https://cwrusdle.bitbucket.io/mds/",
                ontology_graph: Optional[Graph] = None, 
                prefix: Optional[str] = "mds",
                file_events: Optional[bool] = False) -> None:
        """
        Initializes the group with shared project metadata.

        Args:
            proj_name: Name of the project group.
            home_path: Root directory for all child analyses.
            metadata_template: Metadata information about analysis parameters.
            orcid: Researcher's ORCID iD.
            base_uri: Base URI for semantic namespaces.
            ontology_graph: Shared RDFLib Graph.
            prefix: Prefix for the base URI.
            file_events: Option to save file events. Default to False.
        """

        self.analyses = {}
        self.proj_name = proj_name
        self.home_path = home_path
        self.orcid = orcid
        self.base_uri = base_uri
        self.ontology = ontology_graph
        if ontology_graph is None:
            if AnalysisGroup.mds_graph is None:
                print("""
                MDS-Onto from source is not available, please parse ontology from a local file.
                Run AnalysisGroup_instance.ontology.parse('path/to/ontology')
                """)
                user_defined_onto = Graph()
                self.ontology = user_defined_onto
            else:
                self.ontology = AnalysisGroup.mds_graph
        else:
            self.ontology = ontology_graph
        self.prefix = prefix
        self.group_id = f"runGroup{str(uuid4().int)[-15:].zfill(15)}"
        self.QUDT = Namespace("http://qudt.org/schema/qudt/")
        self.MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
        if metadata_template:
            self.metadata_template = metadata_template
        else:
            self.metadata_template = {}
        
        self.metadata_obj = Metadata(self.metadata_template)
        self.store_file_events = file_events

    def get_context(self) -> dict:
        """
        Defines the JSON-LD context for the group metadata.

        Returns:
            dict: Prefix to namespace URI mappings.
        """
        return {
            self.prefix: self.base_uri,
            "qudt": "http://qudt.org/schema/qudt/",
            "mds": "https://cwrusdle.bitbucket.io/mds/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", 
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#", 
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "prov": "http://www.w3.org/ns/prov#",
            "dcterms": "http://purl.org/dc/terms/",
            "cco": "https://www.commoncoreontologies.org/",
            "unit": "https://qudt.org/vocab/unit/",
            "obo": "http://purl.obolibrary.org/obo/"      
        }

    def track(self, func):
        """
        A decorator to automatically wrap a function with provenance tracking.

        Args:
            func: The function to be decorated.

        Returns:
            function: The wrapped function that executes via run_and_track.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.run_and_track(func, *args, **kwargs)
        return wrapper
        

    def run_and_track(self, func, *args, tracker: Optional[AnalysisTracker] = None, **kwargs):
        """
        Executes a function and stores metadata. Can use an existing tracker
        to group multiple functions under one ID, or create a new one.
        """
        
        # 1. Option: Use the injected tracker or create a new instance
        analysis = tracker if tracker is not None else AnalysisTracker(
                        proj_name=self.proj_name, 
                        home_path=self.home_path, 
                        orcid=self.orcid,
                        metadata_template=self.metadata_template,
                        base_uri=self.base_uri,
                        ontology_graph=self.ontology,
                        prefix=self.prefix,
                        file_events=self.store_file_events
                        )

        # 2. Execute the function via the tracker
        analysis_result = analysis.run_and_track(func, *args, **kwargs)
        
        # 3. Update Group-level registries
        # We use the analysis_id as the key. If using the same tracker, 
        # this will update the existing entry rather than creating a new row.
        self.analyses[analysis.analysis_id] = {
                "analysis_obj": analysis,
                "result": analysis_result,
                "jsonld": analysis.create_analysis_jsonld(),
                "report": analysis.create_report(),
                "dataframe": analysis.create_arg_df()
            }
        
        # Generate and update semantic metadata
        analysis_temp, _, _ = analysis.create_metadata_template()
        self.metadata_obj.update_bulk(analysis_temp)
        
        return analysis_result

    def run_and_track_R(self, func, *args, tracker: Optional[AnalysisTracker] = None, **kwargs):
        """
        Executes a function in R and stores metadata. Can use an existing tracker
        to group multiple functions under one ID, or create a new one.
        """
        
        # 1. Option: Use the injected tracker or create a new instance
        analysis = tracker if tracker is not None else AnalysisTracker(
                        proj_name=self.proj_name, 
                        home_path=self.home_path, 
                        orcid=self.orcid,
                        metadata_template=self.metadata_template,
                        base_uri=self.base_uri,
                        ontology_graph=self.ontology,
                        prefix=self.prefix,
                        file_events=self.store_file_events
                        )

        # 2. Execute the function via the tracker
        analysis_result = analysis.run_and_track_R(func, *args, **kwargs)
        
        # 3. Update Group-level registries
        # We use the analysis_id as the key. If using the same tracker, 
        # this will update the existing entry rather than creating a new row.
        self.analyses[analysis.analysis_id] = {
                "analysis_obj": analysis,
                "result": analysis_result,
                "jsonld": analysis.create_analysis_jsonld(),
                "report": analysis.create_report(),
                "dataframe": analysis.create_arg_df()
            }
        
        # Generate and update semantic metadata
        analysis_temp, _, _ = analysis.create_metadata_template()
        self.metadata_obj.update_bulk(analysis_temp)
        
        return analysis_result


    def create_group_arg_df(self) -> pd.DataFrame:
        """
        Aggregates all individual analysis DataFrames into a single 
        master DataFrame.

        Returns:
            pd.DataFrame: Concatenated data from all tracked analyses.
        """
        
        if not self.analyses:
            warnings.warn("No analyses have been tracked in this group yet.")

            return pd.DataFrame()

        df_list = [meta["dataframe"] for meta in self.analyses.values()]

        group_df = pd.concat(df_list, axis=0, ignore_index=True, sort=False)

        cols = group_df.columns.tolist()
        metadata_cols = ["ProjectTitle", "__rowkey__"]

        existing_meta = [c for c in metadata_cols if c in cols]
        data_cols = [c for c in cols if c not in existing_meta]
        result = group_df[existing_meta + data_cols]
        
        return cast(pd.DataFrame, result)

    def create_metadata_template(self):
        """
        Automatically generates a metadata template by matching 
        group data columns against the loaded ontology.

        Returns:
            tuple: (metadata_template, matched_log, unmatched_log)
        """
        group_arg_df = self.create_group_arg_df()

        dummy_mdsdf = MatDatSciDf(df=group_arg_df, ontology_graph=self.ontology, metadata_template={})
        metadata_template, matched_log, unmatched_log = dummy_mdsdf.template_generator(skip_prompts=True)

        return metadata_template, matched_log, unmatched_log

    def create_MatDatSciDf(self):
        """
        Converts the group data into a MatDatSciDf object, integrating 
        ontology-mapped metadata.

        Returns:
            MatDatSciDf: The semantic-aware DataFrame object.
        """
        metadata_template, matched_log, unmatched_log = self.create_metadata_template()
        arg_df = self.create_group_arg_df()

        arg_MatDatSciDf = MatDatSciDf(df = arg_df, 
                                    metadata_template=metadata_template, 
                                    ontology_graph=self.ontology,
                                    matched_log=matched_log, 
                                    unmatched_log=unmatched_log)

        return arg_MatDatSciDf

    def create_group_report(self):
        """
        Consolidates individual analysis reports into one master Markdown document.

        Returns:
            str: A full Markdown report for the entire group.
        """

        group_report = []

        group_report.append(f"# Group Analysis Report: {self.proj_name}")
        group_report.append(f"**Group Analysis ID:** `{self.group_id}`")
        group_report.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        group_report.append("\n---\n")

        for analysis_id, meta in self.analyses.items():
            report = meta["report"]

            group_report.append(report)
        
        group_report.append(f"Generated by FAIRLinked version {__version__}")
        group_report.append("\n---")

        return "\n".join(group_report)

    def save_report(self):
        """
        Saves the consolidated group report to a dedicated group directory.
        """
        report_dir = os.path.join(self.home_path, self.group_id)
        os.makedirs(report_dir, exist_ok=True)
        
        filename = f"{self.proj_name}_{self.group_id}_summary.md"
        full_path = os.path.join(report_dir, filename)
        
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(self.create_group_report())
        
        print(f"Report saved at {full_path}")

    def save_jsonld(self):
        """
        Serializes all individual analysis JSON-LDs and creates a 
        master graph file that links all components to the group activity.
        """
        combined_nodes = []
        # Create a list of references to show "Components" of the group
        analysis_refs = [{"@id": f"mds:{aid}"} for aid in self.analyses.keys()]

        # 1. Loop through the dictionary using .items()
        for analysis_id, meta in self.analyses.items():
            # Trigger the individual tracker's serialization
            meta["analysis_obj"].serialize_analysis_jsonld()
            
            # Load the individual graph
            individual_data = json.loads(meta["jsonld"])
            
            if "@graph" in individual_data:
                for node in individual_data["@graph"]:
                    # Link the primary Analysis Activity to this Group
                    if node["@id"] == f"mds:{analysis_id}":
                        node["group"] = {"@id": f"mds:{self.group_id}"}
                
                combined_nodes.extend(individual_data["@graph"])

        # 2. Define the Group Metadata Node
        group_node = {
            "@id": f"mds:{self.group_id}",
            "@type": "mds:AnalyticalResult",
            "dcterms:title": self.proj_name,
            "dcterms:creator": {"@id": f"https://orcid.org/{self.orcid}"},
            "dcterms:date": datetime.now().strftime("%Y-%m-%d"),
            "mds:hasAnalysisComponent": analysis_refs,
            "mds:hasStudyStage": "Analysis"
        }

        # 3. Final Master Graph Assembly
        master_output = {
            "@context": self.get_context(),
            "@graph": [group_node] + combined_nodes
        }

        # 4. Save to file
        group_json_dir = os.path.join(self.home_path, self.group_id, "_group_json")
        os.makedirs(group_json_dir, exist_ok=True)
        filename = f"{self.proj_name}_{self.group_id}_master_graph.json"
        
        with open(os.path.join(group_json_dir, filename), "w", encoding="utf-8") as f:
            json.dump(master_output, f, indent=2)

        print(f"JSON-LD saved at {os.path.join(group_json_dir, filename)}")

    #### METADATA OBJECT WRAPPERS ####
    def update_metadata(self, col_name: str, field: str, value: str):
        """
        Wrapper to update a metadata property (unit, type, definition, etc.) 
        for a specific column.
        """
        self.metadata_obj.update_template(col_name, field, value)
        self.metadata_template = self.metadata_obj.metadata_temp

    def add_column_metadata(self, col_name: str, rdf_type: str, unit: str = "UNITLESS", 
                            definition: str = "No definition provided", study_stage: str = "UNK"):
        """
        Top-level API to manually define semantic metadata for a new column.
        Useful for defining columns found in 'Discovery Warning' reports.
        """
        existing = self.metadata_obj.add_column_metadata(col_name, rdf_type, unit, definition, study_stage)
        if existing:
            print(f"⚠️ Metadata for '{col_name}' already exists. Use update_metadata instead.")
        self.metadata_template = self.metadata_obj.metadata_temp

    def overwrite_metadata(self, metadata_template: dict):
        """
        Wrapper to delete and replace metadata information. WARNING: THIS WILL DELETE ALL CURRENT METADATA
        """
        new_metadata_obj = Metadata(metadata_template=metadata_template)
        self.metadata_obj = new_metadata_obj
        self.metadata_template = metadata_template

    def delete_column_metadata(self, col_name: str):
        """
        Top-level API to remove a column's semantic metadata definition.
        Useful for cleaning up incorrect mappings or unwanted discovery columns.

        Args:
            col_name (str): The column label to remove from the metadata template.
        """
        self.metadata_obj.delete_column_metadata(col_name)
        self.metadata_template = self.metadata_obj.metadata_temp

    def view_metadata(self, format: str = "table"):
        """
        Wrapper to print the current metadata template as a 
        formatted table or raw JSON-LD. Change to format = 'json-ld' 
        to view metadata template in JSON-LD format.
        """
        self.metadata_obj.print_template(format=format)

    def save_metadata(self, output_path: str, matched_log_path: Optional[str] = None, 
                           unmatched_log_path: Optional[str] = None):
        """
        Wrapper to export the JSON-LD template and the status logs 
        (matched/unmatched columns) to files.
        """
        self.metadata_obj.save_metadata(output_path, matched_log_path, unmatched_log_path)














    

    

    

    

    
