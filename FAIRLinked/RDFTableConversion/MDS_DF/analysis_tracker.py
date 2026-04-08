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
from rdflib import Graph, Namespace
from ...InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import warnings
import requests
from ... import __version__
from .utility import extract_terms_from_ontology, find_best_match, get_curie

class AnalysisTracker:
    """
    A system for auditing scientific analysis, capturing data provenance, 
    and generating semantic JSON-LD metadata.
    """

    mds_graph = load_mds_ontology_graph()

    def __init__(self, 
                proj_name: str, 
                home_path: str, 
                orcid: str = "0000-0000-0000-0000", 
                base_uri: Optional[str] = "https://cwrusdle.bitbucket.io/mds/",
                ontology_graph: Optional[Graph] = None, 
                prefix: Optional[str] = "mds") -> None:
        """
        Initializes the tracker with project metadata and researcher identity.

        Args:
            proj_name: Human-readable name of the research project.
            home_path: Root directory for storing all analysis artifacts.
            orcid: Researcher's ORCID iD for provenance attribution. 
                   Attempts to verify via Public API.
            base_uri: The base URI for semantic namespace generation.
            ontology_graph: A custom RDFLib Graph. Defaults to MDS ontology.
            prefix: The prefix used for the base_uri in JSON-LD.
        """
        
        self.home_path = home_path
        if orcid == "0000-0000-0000-0000":
            self.orcid = orcid
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
        self.analysis_id = f"run_{uuid4().hex[:12]}"
        self.sources = []
        self.proj_name = proj_name
        self.file_events = []
        if ontology_graph is None:
            if MatDatSciDf.mds_graph is None:
                print("MDS-Onto from source is not available, please parse ontology from a local file")
                user_defined_onto = Graph()
                self.ontology = user_defined_onto
            else:
                self.ontology = MatDatSciDf.mds_graph
        else:
            self.ontology = ontology_graph
        
        self.MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
        self.QUDT = Namespace("http://qudt.org/schema/qudt/")
        self.ontology.bind("mds", self.MDS)
        
        

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
        Executes a function while auditing arguments, results, and file system events.

        This method captures:
        1. Function input arguments and default values.
        2. OS-level file handles (reads/writes) active during execution.
        3. The function's return value.

        Args:
            func: The target function to execute.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            Any: The original return value of the tracked function.
        """
        # 1. Log Arguments
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        run_id = f"{func.__name__}.{self.analysis_id}"
        for name, val in bound_args.arguments.items():
            if name == 'self':
                self.track_other("instance_attr", val, parent_id=run_id)
            else:
                # Regular arguments (dataframes, floats, etc.)
                self._route_data(name, val, parent_id=run_id)
        
        # 2. Science Execution with OS-Level Auditing
        process = psutil.Process(os.getpid())
        
        # Execute
        result = func(*args, **kwargs)
        
        # 3. Capture open file handles at the moment of completion
        # Note: Polling in a thread is better for short-lived files, 
        # but this catches all currently active handles.
        for file in process.open_files():
            mode = getattr(file, 'mode', 'r')
                
            event_type = "read/import" if 'r' in mode else "write/modification"
            self.file_events.append({
                    "@id": f"{self.prefix}:file_event_{uuid4().hex[:8]}_{self.analysis_id}",
                    "@type": "cco:ont00000958",
                    "mds:fileName": os.path.basename(file.path),
                    "mds:fileLocation": file.path,
                    "mds:fileEvent": event_type,
                    "prov:generatedAtTime": datetime.now().isoformat()
                })

        if isinstance(result, tuple):
            for i, item in enumerate(result):
                self._route_data(f"{func.__name__}_output_{i}", item, parent_id=run_id)
        else:
            self._route_data(f"{func.__name__}_output", result, parent_id=run_id)
        return result

    def _route_data(self, name, val, parent_id=None):
        """
        The central dispatcher that directs data to specific tracking methods 
        based on the object's type (DataFrame, Dict, List, etc.).

        Args:
            name: The variable name or identifier.
            val: The data object to be tracked.
            parent_id: Optional identifier of the parent container for nesting.
        """
        if isinstance(val, pd.DataFrame):
            self.track_dataframe(name, val, parent_id)
        elif isinstance(val, dict):
            self.track_dict(name, val, parent_id)
        elif isinstance(val, (list, np.ndarray)):
            self.track_list_array(name, val, parent_id)
        elif isinstance(val, (str, int, float, bool)):
            self.track_simple_datatype(name, val, parent_id)
        else:
            self.track_other(name, val, parent_id)


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

        onto_terms = extract_terms_from_ontology(self.ontology)
        
        match = find_best_match(name, ontology_terms=onto_terms)

        bindings = self.get_context()

        iri = match['iri'] if match else None

        curie = get_curie(iri, bindings) 

        self.sources.append({
            "@id": f"{self.prefix}:{name}.{self.analysis_id}",
            "@type": f"{curie}",
            "mds:argumentIdentifier": f"{name}.{self.analysis_id}",
            "skos:altLabel": name,
            "mds:argumentType": type(val).__name__,
            "qudt:value": val,
            "mds:containerIdentifier": parent_id 
        })

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
            "mds:containerIdentifier": parent_id # Links to its container
        })
        for k, v in val.items():
            # Recursively pass the current dict as the new parent
            self._route_data(f"{name}.{k}", v, parent_id=current_id)

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
            "mds:containerIdentifier": parent_id
        })

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
            "mds:containerIdentifier": parent_id
        })

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
            "mds:containerIdentifier": parent_id
        })

        # Inspect the object...
        try:
            for attr_name, attr_val in vars(obj).items():
                if not attr_name.startswith('_') and isinstance(attr_val, (int, float, str, bool)):
                    self._route_data(f"{name}.{attr_name}", attr_val, parent_id=current_id)
        except TypeError:
            pass

    def create_analysis_jsonld(self):
        """
        Assembles all tracked data and file events into a valid JSON-LD string.

        Returns:
            str: A formatted JSON-LD string containing the analysis graph.
        """

        orcid_verification = "ORCID iD verified." if self.orcid_verified else "ORCID iD not verified."

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
            "mds:hasStudyStage": "Analysis"
            }
            ]
    
        }
        return json.dumps(output, indent=2)

    def serialize_analysis_jsonld(self):
        """
        Writes the JSON-LD metadata to a physical file within the analysis directory.
        """
        # 1. Define and create the directory
        json_dir = os.path.join(self.home_path, "analysis_json")
        os.makedirs(json_dir, exist_ok=True)

        # 2. Construct the specific filename
        filename = f"{self.proj_name}_{self.analysis_id}_arguments.json"
        full_path = os.path.join(json_dir, filename)

        # 3. Get the JSON-LD data and write to disk
        jsonld_data = self.create_analysis_jsonld()
        
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(jsonld_data)

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

        # 2. File System Events (The "Paper Trail")
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


class AnalysisGroup:

    """
    Manages a collection of related AnalysisTracker instances, facilitating 
    group-level reporting and master graph generation.
    """

    def __init__(self,
                proj_name: str, 
                home_path: str, 
                orcid: str = "0000-0000-0000-0000", 
                base_uri: Optional[str] = "https://cwrusdle.bitbucket.io/mds/",
                ontology_graph: Optional[Graph] = None, 
                prefix: Optional[str] = "mds") -> None:
        """
        Initializes the group with shared project metadata.

        Args:
            proj_name: Name of the project group.
            home_path: Root directory for all child analyses.
            orcid: Researcher's ORCID iD.
            base_uri: Base URI for semantic namespaces.
            ontology_graph: Shared RDFLib Graph.
            prefix: Prefix for the base URI.
        """

        self.analyses = {}
        self.proj_name = proj_name
        self.home_path = home_path
        self.orcid = orcid
        self.base_uri = base_uri
        self.ontology = ontology_graph
        self.prefix = prefix
        self.group_id = f"run_group_{uuid4().hex[:12]}"
        self.QUDT = Namespace("http://qudt.org/schema/qudt/")
        self.MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")

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
        

    def run_and_track(self, func, *args, **kwargs):
        """
        Creates a new AnalysisTracker instance, executes a function, 
        and stores the resulting metadata in the group registry.

        Args:
            func: The target function to track.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        
        analysis = AnalysisTracker(
                        proj_name=self.proj_name, 
                        home_path=self.home_path, 
                        orcid=self.orcid,
                        base_uri=self.base_uri,
                        ontology_graph=self.ontology,
                        prefix=self.prefix
                        )

        analysis_result = analysis.run_and_track(func, *args, **kwargs)
        analysis_jsonld = analysis.create_analysis_jsonld()
        analysis_report = analysis.create_report()
        analysis_df = analysis.create_arg_df()

        
        self.analyses[analysis.analysis_id] = {
                "analysis_obj": analysis,
                "result": analysis_result,
                "jsonld": analysis_jsonld,
                "report": analysis_report,
                "dataframe": analysis_df
            }

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
    












    

    

    

    

    