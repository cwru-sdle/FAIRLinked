import os
import json
import re
import copy
import random
import string
import warnings
from datetime import datetime, timezone
import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace, XSD
from rdflib.collection import Collection
from rdflib.namespace import RDF, SKOS, OWL, RDFS, DCTERMS
from urllib.parse import quote
import traceback
import requests
from ...InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from typing import Optional, List, Union
from .utility import (
    load_licenses, 
    hash6, 
    resolve_predicate, 
    write_license_triple, 
    normalize, 
    extract_terms_from_ontology, 
    find_best_match, 
    extract_qudt_units, 
    prompt_for_missing_fields,
    load_units
)
import ast
from tqdm import tqdm
from .metadata_manager import Metadata
from .data_relations_manager import DataRelationsDict
import tempfile

class MatDatSciDf:
    """
    A semantic wrapper for Pandas DataFrames in the Materials Data Science domain.

    This class serves as a "Semantic Firewall" for experimental materials data. It 
    bridges tabular data and Linked Data by maintaining synchronized internal objects 
    for measurement data, semantic headers, metadata templates, and column-to-column 
    relationships. It enforces FAIR principles by validating researcher identifiers 
    (ORCID) and ensuring ontological consistency before serialization.

    Attributes:
        df (pd.DataFrame): The cleaned measurement data, stripped of metadata headers.
        header_df (pd.DataFrame): A 3-row buffer (Type, Unit, Study Stage) used for 
            mapping or pre-allocating metadata for the dataset.
        metadata_obj (MatDatSciDf.Metadata): The internal manager handling the 
            RDFLib Graph and JSON-LD template synchronization.
        data_relations (MatDatSciDf.DataRelationsDict): The internal manager for 
            defining semantic links (Object/Datatype properties) between columns.
        orcid (str): Validated ORCID iD of the data curator.
        orcid_verified (bool): Boolean status of curator identity verification.
        df_name (str): Descriptive name for the dataset used in file exports.
        ontology (rdflib.Graph): The reference ontology graph used for fuzzy 
            matching and property resolution.
        base_uri (str): The namespace prefix used for generating semantic subjects.
    """

    mds_graph = load_mds_ontology_graph()

    df_name = "Unnamed_Dataframe"
    
    def __init__(self, 
                df: pd.DataFrame, 
                metadata_template: Optional[dict] = None,
                matched_log: Optional[list]= None,
                unmatched_log: Optional[list] = None,
                data_relations_dict: Optional[dict] = None, 
                orcid: str = "0000-0000-0000-0000", 
                df_name: Optional[str] = None,
                metadata_rows: Optional[bool] = False,
                ontology_graph: Optional[Graph] = None, 
                base_uri="https://cwrusdle.bitbucket.io/mds/",
                local_unit_file: Optional[bool] = True):
        """
        Initializes the MatDatSciDf instance, validates identity, and constructs semantic objects.

        Args:
            df (pd.DataFrame): The source DataFrame containing experimental results.
            metadata_template (dict, optional): The initial JSON-LD dictionary defining column contexts.
            matched_log (list, optional): A historical record of columns successfully mapped to 
                ontology terms during the initialization process.
            unmatched_log (list, optional): A record of columns that failed to find an automated 
                match in the reference ontology.
            data_relations_dict (dict, optional): A dictionary of Subject-Predicate-Object 
                mappings to link columns. Defaults to an empty dict.
            orcid (str, optional): The curator's ORCID iD. Validated via public API 
                unless placeholder is used. Defaults to '0000-0000-0000-0000'.
            df_name (str, optional): Custom name for the dataset. 
            metadata_rows (bool, optional): If True, treats the first 3 rows of the input 
                'df' as semantic headers. If False, pre-allocates a blank header table.
            ontology_graph (Graph, optional): Custom RDFLib Graph. If None, uses the 
                package-level MDS ontology.
            base_uri (str, optional): Base URI for RDF @id generation.
            local_unit_file: (bool, optional): Get units directly from QUDT units file in package or get from QUDT website

        Raises:
            warnings.warn: If the ORCID cannot be verified via API due to connection 
                errors or invalid IDs, the curator is tagged as 'UNVERIFIED'.
        """
        

        if metadata_rows is False:
            self.metadata_rows_skip = 0
            self.header_df = pd.DataFrame(index=range(3), columns=df.columns)
        else:
            self.metadata_rows_skip = 3
            self.header_df = df.iloc[:self.metadata_rows_skip]

        skip_rows = self.metadata_rows_skip

        if local_unit_file:
            self.units = load_units()
        else:
            self.units = extract_qudt_units()


        self.df = df.iloc[skip_rows:]
        self.metadata_template = metadata_template if metadata_template else {}

        if ontology_graph is None:
            if MatDatSciDf.mds_graph is None:
                print("""
                MDS-Onto from source is not available, please parse ontology from a local file.
                Run MDS_DF_instance.ontology.parse('path/to/ontology')
                """)
                user_defined_onto = Graph()
                self.ontology = user_defined_onto
            else:
                self.ontology = MatDatSciDf.mds_graph
        else:
            self.ontology = ontology_graph
        
        if not metadata_template or metadata_template == {}:
            template, matched, unmatched = self.template_generator(skip_prompts=True)
            self.metadata_template = template
            self.matched_log = matched
            self.unmatched_log = unmatched
        else:
            self.metadata_template = metadata_template
            self.matched_log = matched_log
            self.unmatched_log = unmatched_log


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
                    warnings.warn(f"❌ ORCID '{orcid}' not found. Data will be marked as UNVERIFIED.")
                    self.orcid = clean_orcid
                    self.orcid_verified = False
            
            except requests.exceptions.RequestException:
                warnings.warn("🌐 Connection Error: Could not verify ORCID. Tagging as UNVERIFIED.")
                self.orcid = orcid
                self.orcid_verified = False

        if df_name is None:
            self.df_name = MatDatSciDf.df_name
        else:
            self.df_name = df_name


        

        self.base_uri = base_uri

        self.MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
        self.ontology.bind("mds", self.MDS)
        if data_relations_dict is None:
            data_relations_dict = {}


        self.data_relations = DataRelationsDict(prop_col_pair_dict=data_relations_dict)
        self.metadata_obj = Metadata(metadata_template=self.metadata_template, matched_log=self.matched_log, unmatched_log=self.unmatched_log)
        init_data_relations_dict = self.get_relation_pairs_onto()
        self.add_relations(data_relations=init_data_relations_dict)
  
        

    def get_relations(self):
        """
        Extracts all Object and Datatype properties from the associated ontology.

        This method scans the ontology graph for OWL ObjectProperties and 
        DatatypeProperties, mapping their human-readable rdfs:labels to their 
        full URIs and property types.

        Returns:
            dict: A dictionary (prop_metadata_dict) where:
                - Key: Property label (str)
                - Value: Tuple of (Property URI, Property Type)
        """

        ontology_graph = self.ontology
        prop_metadata_dict = {}

        for prop_type, label_type in [(OWL.ObjectProperty, "Object Property"), (OWL.DatatypeProperty, "Datatype Property")]:
            for prop in ontology_graph.subjects(RDF.type, prop_type):
                label = ontology_graph.value(prop, RDFS.label)
                if label:
                    prop_metadata_dict[str(label)] = (str(prop), label_type)

        return prop_metadata_dict

    def view_relations(self):
        """
        Prints a formatted list of all semantic relations available in the ontology.

        This is a helper method for users to discover which properties can be 
        used in a DataRelationsDict to link columns together.
        """

        view_all_props = self.get_relations()
        for key, value in view_all_props.items():
            print(f"{key}: {value}")


    ### DATA RELATIONS DICT WRAPPER ###

    def get_relation_pairs_onto(self):
        """
        Analyzes the ontology and metadata template to discover relationships 
        between columns.
        
        Returns:
            dict: { URI: [(subj_col, obj_col), ...] }
        """
        ontology = self.ontology
        metadata_obj = self.metadata_obj
        template_graph = metadata_obj.metadata_temp.get("@graph", [])
        
        # 1. Map column names (altLabels) to their expanded Ontology Class URIs
        col_to_type = {}
        for item in template_graph:
            alt_label = item.get("skos:altLabel")
            rdf_type = item.get("@type")
            
            if alt_label and rdf_type:
                try:
                    # Use the graph's namespace manager to expand CURIEs like 'mds:Result'
                    type_uri = ontology.namespace_manager.expand_curie(rdf_type)
                    col_to_type[alt_label] = type_uri
                except Exception:
                    continue

        relation_dict = {}
        columns = list(col_to_type.keys())

        # Helper to extract classes from range (handling owl:unionOf)
        def get_classes_in_range(prop_uri):
            classes = set()
            for r in ontology.objects(prop_uri, RDFS.range):
                union_node = ontology.value(r, OWL.unionOf)
                if union_node:
                    classes.update(list(Collection(ontology, union_node)))
                else:
                    classes.add(r)
            return classes

        # 2. Iterate through columns and ontology to find valid links
        for s_col in columns:
            s_type = col_to_type[s_col]
            s_hierarchy = list(ontology.transitive_objects(s_type, RDFS.subClassOf))
            
            for s_cls in s_hierarchy:
                for prop in ontology.subjects(RDFS.domain, s_cls):
                    if (prop, RDF.type, OWL.DatatypeProperty) in ontology:
                        continue
                    
                    valid_ranges = get_classes_in_range(prop)
                    
                    for o_col in columns:
                        if s_col == o_col: 
                            continue
                            
                        o_type = col_to_type[o_col]
                        o_hierarchy = set(ontology.transitive_objects(o_type, RDFS.subClassOf))
                        
                        if any(r_cls in o_hierarchy for r_cls in valid_ranges):
                            prop_str = str(prop)
                            
                            if prop_str not in relation_dict:
                                relation_dict[prop_str] = []
                            
                            pair = (s_col, o_col)
                            if pair not in relation_dict[prop_str]:
                                relation_dict[prop_str].append(pair)

        return relation_dict
    
    def add_relations(self, data_relations: dict):
        data_rel_obj = self.data_relations
        onto_graph = self.ontology
        onto_props = self.get_relations()
        data_rel_obj.add_relations(data_relations=data_relations, 
                                ontology_graph=onto_graph, 
                                onto_props=onto_props)

    def delete_relation(self, prop_key: str, pair: Optional[tuple] = None):
        """
        Top-level API to remove semantic links between columns.

        Args:
            prop_key (str): The property identifier (e.g., 'mds:measuredBy').
            pair (tuple, optional): Specific (subj, obj) columns to un-link. 
                If None, removes all links for that property.
        """
        self.data_relations.delete_relation(prop_key, pair)

    def validate_data_relations(self):
        """Wrapper to validate relations using the instance's own data and ontology."""
        onto_metadata = self.get_relations()
        data_rel_obj = self.data_relations
        return data_rel_obj.validate_data_relations(self.df, self.ontology, onto_metadata)

    def view_data_relations(self):
        """
        Displays a visual validation report for the provided DataRelationsDict.
        """
        self.data_relations.print_data_relations(
            df=self.df, 
            df_name = self.df_name,
            ontology_graph=self.ontology, 
            onto_props=self.get_relations()
        )


    #### METADATA OBJECT WRAPPERS ####
    def update_metadata_bulk(self, metadata_template: dict):
        """
        Wrapper to update metadata template in bulk for multiple columns
        """
        self.metadata_obj.update_bulk(metadata_template)
        self.metadata_template = self.metadata_obj.metadata_temp

    def update_metadata(self, col_name: str, field: str, value: str):
        """
        Updates a specific property of a column metadata entry in both the JSON-LD template 
        and the internal RDFLib Graph in a synchronized, lock-step transaction.

        This method maps a user-friendly shorthand token (passed via `field`) to its 
        corresponding JSON-LD schema key and formal RDF ontology predicate. It safely 
        modifies the temporary JSON source dictionary and updates the corresponding triple 
        statement within the `template_graph`.

        Parameters
        ----------
        col_name : str
            The exact string name of the target data column (e.g., 'systolic_bp'). 
            Matches against the existing 'skos:altLabel' identifier.
        field : {'definition', 'unit', 'type', 'stage', 'note'}
            The shorthand token representing the metadata property to modify:
            
            * 'definition' : Maps to `skos:definition` (SKOS.definition). Updates the text-based 
                human description. Expects a plain string.
            * 'unit'       : Maps to `qudt:hasUnit` (QUDT.hasUnit). Updates the measurement unit. 
                Accepts a raw value (e.g., 'KG') or a prefixed URI (e.g., 'unit:KG'). Will be 
                transformed into a dictionary block in JSON-LD and a URIRef in RDF.
            * 'type'       : Maps to `@type` / `rdf:type` (RDF.type). Updates the semantic class or 
                concept type of the column. Autocompletes to the 'mds:' namespace if a prefix is missing.
            * 'stage'      : Maps to `mds:hasStudyStage` (MDS.hasStudyStage). Updates the phase of the 
                study lifecycle. Expects a string (e.g., 'COLLECTION').
            * 'note'       : Maps to `skos:note` (SKOS.note). Appends an administrative or usage 
                note to the concept. Expects a string value.
        value : str
            The new data value to assign to the specified field.

        Returns
        -------
        None

        Raises
        ------
        Prints a warning message if the `field` is unrecognized, or if the `col_name` was successfully 
        updated in the JSON template but could not be found as a subject node inside the RDF Graph.
        """
        self.metadata_obj.update_template(col_name, field, value)
        self.metadata_template = self.metadata_obj.metadata_temp

    def add_column_metadata(self, col_name: str, rdf_type: str, unit: str = "UNITLESS", 
                            definition: str = "Definition not available", study_stage: str = "UNK"):
        """
        Registers and appends metadata for a specific data column to both the temporary 
        JSON-LD graph and the internal RDFLib Graph.

        This method prevents duplicate entries by checking the existing JSON-LD `@graph` 
        for the column name. If the column does not exist, it constructs a clean Python 
        dictionary representing the JSON-LD entity, appends it to the temporary graph 
        structure, and synchronizes it by parsing it into the internal `template_graph`.

        Parameters
        ----------
        col_name : str
            The exact name of the data column (e.g., 'patient_age'). Used as the 
            `skos:altLabel` identifier to prevent duplicate entries.
        rdf_type : str
            The RDF semantic type or class for the column. If a namespace prefix (like 'mds:') 
            is omitted, the 'mds:' prefix will be automatically prepended.
        unit : str, optional
            The measurement unit of the column data, mapped to a QUDT ontology identifier. 
            Defaults to "UNITLESS".
        definition : str, optional
            A human-readable textual description of what the column represents. 
            Defaults to "Definition not available".
        study_stage : str, optional
            The phase or stage of the study lifecycle this data belongs to (e.g., 'COLLECTION', 
            'ANALYSIS'). Defaults to "UNKNOWN".

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If required parameters are malformed (handled by downstream JSON/RDF parsers).
        """
        existing = self.metadata_obj.add_column_metadata(col_name, rdf_type, unit, definition, study_stage)
        if existing:
            print(f"⚠️ Metadata for '{col_name}' already exists. Use update_metadata instead.")
        self.metadata_template = self.metadata_obj.metadata_temp

    def delete_column_metadata(self, col_name: str):
        """
        Top-level API to remove a column's semantic metadata definition.
        Useful for cleaning up incorrect mappings or unwanted discovery columns.

        Args:
            col_name (str): The column label to remove from the metadata template.
        """
        self.metadata_obj.delete_column_metadata(col_name)
        self.metadata_template = self.metadata_obj.metadata_temp

    def overwrite_metadata(self, metadata_template: dict):
        """
        Wrapper to delete and replace metadata information. WARNING: THIS WILL DELETE ALL CURRENT METADATA
        """
        new_metadata_obj = Metadata(metadata_template=metadata_template)
        self.metadata_obj = new_metadata_obj
        self.metadata_template = metadata_template

    def view_metadata(self, format: str = "table"):
        """
        Prints the current metadata template to the standard output.

        Depending on the chosen format, this method will either output a pretty-printed 
        JSON-LD structure representing the underlying knowledge graph or a tabular 
        summary compiled into a pandas DataFrame.

        Parameters
        ----------
        format : {'table', 'json'}, default 'table'
            The output format for displaying the metadata template.
            
            * 'table': Flattens the nested JSON-LD '@graph' arrays (including handling 
                complex structures like 'qudt:hasUnit' sub-dictionaries) and extracts key 
                attributes (`Label`, `Type`, `Unit`, `Definition`, `Study Stage`) into a 
                summarized, human-readable table. If executed in a Jupyter Notebook, it 
                renders as an HTML table; in a terminal, it outputs as plain text.
            * 'json': Outputs the raw, un-flattened JSON-LD template structure with 
                proper indentation for deep debugging.

        Returns
        -------
        None

        Outputs
        -------
        Prints the formatted metadata summary or raw JSON directly to stdout. If an 
        unsupported format string is provided, prints an error message.
        """
        self.metadata_obj.print_template(format=format)

    def save_metadata(self, output_path: str, matched_log_path: Optional[str] = None, 
                           unmatched_log_path: Optional[str] = None):
        """
        Wrapper to export the JSON-LD template and the status logs 
        (matched/unmatched columns) to files.
        """
        self.metadata_obj.save_metadata(output_path, matched_log_path, unmatched_log_path)

    #### HELPER FUNCTIONS OF MatDatSciDf ####

    def validate_metadata(self) -> bool:
        """
        Performs a two-way integrity check between the DataFrame and the Metadata Template.

        Category 1 (Undefined Data Columns): 
           Columns in the DataFrame that are NOT defined in the Metadata. 
           -> Result: These will be skipped during serialization.
        
        Category 2 (Empty Metadata Entries): 
           Definitions in the Metadata that have no matching column in the DataFrame. 
           -> Result: These will create 'empty' RDF nodes with no measurement values.

        Returns:
            bool: True if data and metadata are perfectly aligned, False otherwise.
        """
        print(f"\n--- 📋 Metadata/DataFrame Alignment Report: {self.df_name} ---")
        
        # 1. Get the sets of labels
        template_graph = self.metadata_template.get("@graph", [])
        template_labels = {item.get("skos:altLabel") for item in template_graph if "skos:altLabel" in item}
        
        # Filter out internal/helper columns from the DF set
        internal_cols = {"__source_file__", "__rowkey__", "__Label__"}
        df_columns = set(self.df.columns) - internal_cols
        
        all_clear = True

        # --- CHECK 1: Undefined Data Columns (Data without a definition) ---
        undefined_cols = df_columns - template_labels
        if undefined_cols:
            print(f"❌ [UNDEFINED COLUMNS] {len(undefined_cols)} columns in the DataFrame have no metadata.")
            print("Note: These data columns will be ignored during serialization.")
            for col in undefined_cols:
                print(f"   └─ {col}")
            all_clear = False
        else:
            print("✅ [UNDEFINED COLUMNS] All DataFrame columns are defined in the metadata.")

        # --- CHECK 2: Empty Metadata Entries (Definitions without data) ---
        empty_entries = template_labels - df_columns
        if empty_entries:
            print(f"⚠️  [EMPTY ENTRIES] {len(empty_entries)} metadata definitions have no matching data columns.")
            print("Note: These will result in RDF nodes missing 'qudt:value' triples.")
            for col in empty_entries:
                print(f"   └─ {col}")
        else:
            print("✅ [EMPTY ENTRIES] All metadata definitions have matching data columns.")

        # --- CHECK 3: [MISSING FIELDS] (Semantic incompleteness) ---
        problematic_items = []
        for item in template_graph:
            label = item.get("skos:altLabel", "Unnamed Column")
            missing = []
            
            # Check for missing @type
            if not item.get("@type"):
                missing.append("@type")
            
            # Check for missing/default definition
            definition = str(item.get("skos:definition", "")).lower()
            if not definition or "not available" in definition:
                missing.append("skos:definition")

            if missing:
                problematic_items.append((label, missing))

        if problematic_items:
            print(f"⚠️  [MISSING FIELDS] {len(problematic_items)} definitions are incomplete.")
            for label, missing_list in problematic_items:
                # This prints: └─ Column_Name (Missing: @type, skos:definition)
                print(f"   └─ {label} (Missing: {', '.join(missing_list)})")
            all_clear = False
        else:
            print("✅ [MISSING FIELDS] All metadata entries are semantically complete.")

        print("-----------------------------------------------------------\n")
        return all_clear

    def template_generator(self, skip_prompts: bool = False):
        """
        Generates a semantic metadata template by mapping DataFrame columns to ontology terms.

        This method performs a fuzzy match between column headers and the loaded ontology. 
        It attempts to automatically resolve the RDF type (@type), study stage, 
        and units. If a direct match is not found, or if 'skip_prompts' is False, 
        it can interactively prompt the user to provide missing metadata fields.

        The resulting template follows the JSON-LD structure, integrating namespaces 
        such as QUDT, SKOS, PROV, and MDS.

        Args:
            skip_prompts (bool, optional): If True, suppresses interactive user input 
                for missing units or definitions, instead using 'UNITLESS' or 
                placeholders. Defaults to False.

        Returns:
            tuple: A tuple containing:
                - metadata_template (dict): The complete JSON-LD dictionary with 
                  '@context' and '@graph' entries for each column.
                - matched_log (list): A list of strings documenting successful 
                  fuzzy-match associations (Column => IRI).
                - unmatched_log (list): A list of column names that could not be 
                  found in the provided ontology.

        Note:
            - The method prioritizes metadata explicitly included in the first three 
              rows of the CSV (type, unit, study stage).
            - Unit extraction handles both raw strings (e.g., 'unit:KiloGM') and 
              string-encoded dictionaries (e.g., "{'@id': 'unit:M'}").
            - Time-stamping via 'prov:generatedAtTime' is applied to each entry for 
              provenance tracking.
        """

        h_df = self.header_df
        ontology_graph = self.ontology

        columns = h_df.columns
        ontology_terms = extract_terms_from_ontology(ontology_graph)

        bindings_dict = {prefix: str(namespace) for prefix, namespace in ontology_graph.namespaces()}
        if "mds" not in bindings_dict:
            bindings_dict["mds"] = "https://cwrusdle.bitbucket.io/mds/"

        matched_log = []
        unmatched_log = []
        bindings = {}

        jsonld = {
        "@context": {
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
            "obo": "http://purl.obolibrary.org/obo/"      
        },
        "@graph": []
        }   

        units = self.units

        for col in columns:
            if col == "__source_file__" or col == "__Label__" or col == "__rowkey__":
                continue
            typ = h_df.loc[0,col]

            match = find_best_match(col, ontology_terms)
            if(pd.isna(typ) or ":" not in typ):# if no type was explicitily included in csv
            
                #get iri from closest match
                iri_fragment = str(match["iri"]).split("/")[-1].split("#")[-1] if match else normalize(col)

                # Get base iri
                iri_str = str(match["iri"]) if match else None
                binding =""
                study_stage = ""
                definition = "Definition not available"
                if iri_str:
                    last_slash = iri_str.rfind("/")
                    last_hash = iri_str.rfind("#")
                    split_pos = max(last_slash, last_hash)
                    iri_base = iri_str[:split_pos + 1] if split_pos != -1 else iri_str
                    binding = next((k for k, v in bindings_dict.items() if v == iri_base), "mds")
                    
                    #add binding to list of contexts
                    if(binding not in bindings):
                        bindings[binding] = bindings_dict[binding]

                    definition = str(match["definition"]) if match else "Definition not available"
                    study_stage = match["study_stage"][0].value if (match and match.get("study_stage")) else "Study stage information not available"
                
            else: #csv included type:
                binding, iri_fragment = typ.split(":")
                if(binding == "mds"):
                    #if term in mds ontology, get study stage and def from ontology
                    definition = str(match["definition"]) if match else "Definition not available"
                    study_stage = match["study_stage"][0].value if (match and match.get("study_stage")) else "Study stage information not available"
                else:
                    definition = "Definition not available"
                    study_stage = h_df.loc[2,col] #try to get study stage from csv
                    if pd.isna(study_stage): 
                        study_stage =  "Study stage information not available"

            # try get units
            un = h_df.loc[1,col]
            if not pd.isna(un):
                try:
                    # Step 1: Convert string representation to a Python object
                    # Handles both "{'id': ...}" and "unit:UNIT"
                    if un.startswith("{") and un.endswith("}"):
                        try:
                            evaluated = ast.literal_eval(un)
                            if isinstance(evaluated, dict):
                                # It's a dict, get the ID value
                                target_str = evaluated.get('@id', "")
                            else:
                                target_str = un
                        except Exception:
                            target_str = un

                    else:
                        # It's already a string (like "unit:UNIT")
                        target_str = str(un).strip()

                    # Step 3: Split and safely get the second part
                    if ":" in target_str:
                        un = target_str.split(":")[1]
                    else:
                        matches = []
                        for key, details in units.items():
                            # Clean up comparisons using .lower() and check both ucum and labels
                            name = details.get('name', '')
                            ucum_code = details.get('ucum_code', '')
                            label = details.get('label', '')
                            
                            if ((name and un.lower() == name.lower()) or
                                (ucum_code and un == ucum_code) or 
                                (label and un.lower() == label.lower())):
                                matches.append(key)

                        if len(matches) == 1 :
                            un = matches[0]

                        elif len(matches) > 1:
                            print(f"\nMultiple matches found for unit {un}. Select a number or click 'enter' to skip. \n'UNITLESS' will be used if user does not select a unit:")
                            for i, m in enumerate(matches, 1):
                                print(f"  {i}. {units[m]['label']} ({m})")
                            choice = input("> ")
                            if choice.lower() == '':
                                print("No unit chosen. Default to UNITLESS")
                                un = "UNITLESS"
                            if choice.isdigit() and 1 <= int(choice) <= len(matches):
                                un = matches[int(choice) - 1]
                        else:
                            print(f"Unable to find unit {un}. Default to UNITLESS")
                            un = "UNITLESS"
                        


                except (ValueError, SyntaxError, IndexError) as e:
                    print(f"Parsing error for value '{un}': {e}")
                    un = "UNITLESS"
            else:
                un = "UNITLESS"
                
            if match:
                matched_log.append(f"{col} => {iri_fragment}")

            else:
                unmatched_log.append(col)


            
            if not skip_prompts:
                unit, study, notes = prompt_for_missing_fields(iri_fragment, un, study_stage, ontology_graph, units)
            else:
                unit = un
                study = study_stage if study_stage not in [
                    "", "Study stage information not available"
                ] else ""
                notes = ""
            

            if(binding == ""):
                binding = "mds"

            if(binding not in bindings):
                        bindings[binding] = bindings_dict[binding]
            
            entry = {
                "@id": f"{binding}:{iri_fragment}",
                "@type": f"{binding}:{iri_fragment}",
                "skos:altLabel": col,
                "skos:definition": definition,
                "qudt:hasUnit": {"@id": f"unit:{unit}"},
                "prov:generatedAtTime": {
                    "@value": datetime.now().astimezone().isoformat(),
                    "@type": "xsd:dateTime"
                },
                "skos:note": {
                    "@value": f"{notes}",
                    "@language": "en"
                },
                "mds:hasStudyStage": study
            }
            jsonld["@graph"].append(entry)
        
        jsonld["@context"].update({
            "unit": "https://qudt.org/vocab/unit/"
            })
        for i in bindings:
            jsonld["@context"].update({
                i: bindings[i]
            })

        metadata_template = jsonld

        return metadata_template, matched_log, unmatched_log

    #### SERIALIZE INTO LINKED DATA #####     
    def semantic_remapping(self, data_graph: Graph):
        """
        Validates types against the reference ontology and remaps 
        unrecognized types to the base BFO Entity class.
        """
        
        # 1. Define the OBO Namespace and the BFO Entity class
        OBO = Namespace("http://purl.obolibrary.org/obo/")
        BFO_ENTITY = OBO.BFO_0000001
        
        # 2. Ensure OBO is bound for clean serialization
        if data_graph.namespace_manager.store.prefix(URIRef("http://purl.obolibrary.org/obo/")) is None:
            data_graph.bind("obo", OBO)

        # 3. Cache valid classes from your reference ontology for speed
        ref_classes = set(self.ontology.subjects(predicate=RDF.type, object=OWL.Class))
        ref_classes.update(self.ontology.subjects(predicate=RDF.type, object=RDFS.Class))

        # 4. Find all subjects that have an rdf:type
        # We iterate over triples to identify which specific subjects need remapping
        for s, p, o in data_graph.triples((None, RDF.type, None)):
            if o not in ref_classes:
                print(f"⚠️ Warning: {o} not in reference ontology. Remapping {s} to obo:BFO_0000001")
                
                # .set() removes all existing (s, RDF.type, ...) and adds (s, RDF.type, BFO_ENTITY)
                data_graph.set((s, RDF.type, BFO_ENTITY))
    
        return data_graph

                

    def serialize_row(self, 
                    output_folder: str, 
                    format = 'json-ld', 
                    row_key_cols: Optional[list[str]] = None, 
                    id_cols: Optional[list[str]] = None,
                    label_pairs: Optional[list[tuple[str, str]]] = None, 
                    license: Optional[str]= None,
                    write_files: Optional[bool] = True) -> list[Graph]:

        """
        Serializes each row of the DataFrame into individual RDF files using the 
        active semantic metadata template.
        """

        df = self.df
        orcid = self.orcid
        metadata_obj = self.metadata_obj
        data_relation_dict = self.data_relations
        prop_column_pair_dict = data_relation_dict.prop_pair_dict if data_relation_dict else None
        results = []
        ontology_graph = self.ontology
        metadata_template = metadata_obj.metadata_temp
        base_uri = self.base_uri
        context = metadata_template.get("@context", {})
        graph_template = metadata_template.get("@graph", [])
        prop_metadata_dict = self.get_relations()

        if write_files:
            os.makedirs(output_folder, exist_ok=True)

        sskey = {
            "Synthesis": "SYN", 
            "Formulation": "FOR", 
            "Material Processing": "MAT_PRO",
            "Sample": "SA", 
            "Tool": "TL", 
            "Recipe": "REC", 
            "Result": "RSLT", 
            "Analysis": "AN", 
            "Modeling": "MOD",
            "": "UNK"
        }

        rowpredicate = URIRef("https://cwrusdle.bitbucket.io/mds/row")

        # check license
        if not license:
            license_uri = URIRef("https://spdx.org/licenses/CC0-1.0.html")
            print("No license provided. Default to CC0-1.0 (Public Domain)")

        elif not license.startswith("http"):
            spdx_data = load_licenses()
            valid_ids = {lic["licenseId"] for lic in spdx_data["licenses"]}

            if license not in valid_ids:
                raise ValueError(
                    f"Invalid SPDX license ID '{license}'.\n"
                    f"Please use one from https://spdx.org/licenses/."
                )

            license_uri = URIRef(f"https://spdx.org/licenses/{license}.html")
            write_license_triple(output_folder, base_uri, license_uri)

        else:
            license_uri = URIRef(license)
            write_license_triple(output_folder, base_uri, license_uri)


        for idx, row in df.iterrows():
            try:
                # Deep copy the template and assign @id
                template_copy = copy.deepcopy(graph_template)
                subject_lookup = {}  # Maps skos:altLabel → generated @id

                # Generate row key
                c = [item["skos:altLabel"] for item in template_copy if "skos:altLabel" in item]
                if row_key_cols is None or not any(x in c for x in row_key_cols):
                    keys = {}
                    for item in template_copy:
                        if "skos:altLabel" not in item or not item["skos:altLabel"]:
                            raise ValueError("Missing skos:altLabel in template")
                        col = item["skos:altLabel"]
                        studystage = item.get("mds:hasStudyStage", "")
                        val = df.at[idx, col]

                        if studystage not in keys:
                            keys[studystage] = [val]
                        else:
                            keys[studystage].append(val)
                    
                    row_key = ""
                    for key in keys:
                        try:
                            num = str(hash6("".join([str(x) for x in keys[key] if not pd.isna(x)])))
                            row_key += sskey.get(key, "UNK") + num + "-"
                        except Exception:
                            traceback.print_exc()
                else:
                    row_key = ""
                    for x in set(c) & set(row_key_cols):
                        row_key += str(df.at[idx, x]).strip() + "-"
                
                orcid_rk = orcid.replace("-", "")
                full_row_key = f"{row_key}or{orcid_rk}".replace(" ", "")
                clean_row_key = row_key.rstrip("-")

                # ==========================================
                # Prepare Template Fields & Subject URIs
                # ==========================================
                for item in template_copy:
                    if "@type" not in item or not item["@type"]:
                        warnings.warn(f"Missing or empty @type in template item: {item}")
                        continue
                    if "skos:altLabel" not in item or not item["skos:altLabel"]:
                        raise ValueError("Missing skos:altLabel in template")

                    raw_type = str(item["@type"]).strip()
    
                    if "://" in raw_type:
                        # Full IRI (e.g., "https://cwrusdle.bitbucket.io/mds/ShearRate")
                        localname = raw_type.split("/")[-1].split("#")[-1]
                    elif ":" in raw_type:
                        # CURIE (e.g., "mds:ShearRate")
                        _, localname = raw_type.split(":", 1)
                    else:
                        # Bare string (e.g., "ShearRate")
                        localname = raw_type

                    if id_cols is not None and item["skos:altLabel"] in id_cols:
                        raw_identifier = row.get(item["skos:altLabel"])
                        if not raw_identifier or pd.isna(raw_identifier):
                            warnings.warn(f"Cannot find entity identifier in row {idx}")
                            continue
                        entity_identifier = normalize(re.sub(r'[^a-zA-Z0-9_\-\.]', '', str(raw_identifier)))
                        subject_uri = self.MDS[f"{localname}.{entity_identifier}"]
                    else:
                        subject_uri = self.MDS[f"{localname}.{clean_row_key}"]

                    item["@id"] = URIRef(subject_uri)
                    subject_lookup[item["skos:altLabel"]] = URIRef(subject_uri)

                    if "prov:generatedAtTime" in item and isinstance(item["prov:generatedAtTime"], dict):
                        item["prov:generatedAtTime"]["@value"] = datetime.now(timezone.utc).isoformat() + "Z"

                    if "qudt:hasUnit" in item and not item["qudt:hasUnit"].get("@id"):
                        del item["qudt:hasUnit"]
                    if "qudt:hasQuantityKind" in item and not item["qudt:hasQuantityKind"].get("@id"):
                        del item["qudt:hasQuantityKind"]

                jsonld_data = {
                    "@context": context,
                    "@graph": template_copy
                }

                # Convert to RDF Graph
                g = Graph(identifier=URIRef(f"{base_uri}{full_row_key}{idx}"))
                g.parse(data=json.dumps(jsonld_data), format="json-ld")
                
                QUDT = Namespace("http://qudt.org/schema/qudt/")
                MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
                OBO = Namespace("http://purl.obolibrary.org/obo/")
                
                g.bind("mds", MDS)
                g.bind("qudt", QUDT)
                g.bind("dcterms", DCTERMS)
                g.bind("obo", OBO)

                # Add triples from DataFrame row values
                for alt_label, subj_uri in subject_lookup.items():
                    if alt_label in row:
                        g.remove((subj_uri, QUDT.value, None))
                        if pd.notna(row[alt_label]) and str(row[alt_label]).strip() != "":
                            data_value = row[alt_label]
                            if hasattr(data_value, 'item'):
                                data_value = data_value.item()
                            g.add((subj_uri, QUDT.value, Literal(data_value, datatype=XSD.string)))
                        else:
                            print(f"Skipping NA value for {alt_label} on row {idx} with row key {clean_row_key}")
                        
                        g.add((subj_uri, rowpredicate, Literal(clean_row_key)))
                        g.add((subj_uri, DCTERMS.license, license_uri))
                        curator_uri = URIRef(f"https://orcid.org/{self.orcid}")
                        g.add((subj_uri, DCTERMS.creator, curator_uri))
                        
                        if not getattr(self, 'orcid_verified', True):
                            g.add((subj_uri, SKOS.note, Literal("Caution: Data curator ORCID was not verified at time of serialization.")))

                # ==========================================
                # Process Custom RDFS Label Pairs
                # ==========================================
                if label_pairs:
                    for subj_col, label_col in label_pairs:
                        subj_uri = subject_lookup.get(subj_col)
                        if subj_uri and label_col in row:
                            label_val = row[label_col]
                            if pd.notna(label_val) and str(label_val).strip() != "":
                                if hasattr(label_val, 'item'):
                                    label_val = label_val.item()
                                g.add((subj_uri, RDFS.label, Literal(str(label_val).strip(), datatype=XSD.string)))

                # ==========================================
                # Add Object & Datatype Properties
                # ==========================================
                if prop_column_pair_dict:
                    for key, column_pair_list in prop_column_pair_dict.items():
                        prop_uri, prop_type = resolve_predicate(key, ontology_graph)

                        if prop_uri is None:
                            prop_metadata = prop_metadata_dict.get(key)
                            if not prop_metadata:
                                continue
                            prop_uri, prop_type = prop_metadata
                            pred_uri = URIRef(prop_uri)
                        else:
                            pred_uri = URIRef(prop_uri)

                        for subj_col, obj_col in column_pair_list:
                            if subj_col not in row or pd.isna(row[subj_col]):
                                continue
                            
                            subj_uri = subject_lookup.get(subj_col)
                            if not subj_uri:
                                continue
                            
                            obj_val = row[obj_col]
                            if hasattr(obj_val, 'item'):
                                obj_val = obj_val.item()
                            if pd.isna(obj_val):
                                continue

                            if prop_type == "Object Property":
                                obj_uri = subject_lookup.get(obj_col)
                                if obj_uri is None:
                                    obj_val_str = str(obj_val).strip()
                                    obj_uri = URIRef(f"{base_uri}{quote(obj_val_str, safe='')}")
                                g.add((subj_uri, pred_uri, obj_uri))
                            elif prop_type == "Datatype Property":
                                g.add((subj_uri, pred_uri, Literal(obj_val)))

                # Remove empty QUDT values
                triples_to_remove = [
                    (s, p, o) for s, p, o in g 
                    if p == QUDT.value and len(str(o).strip()) == 0
                ]
                for triple in triples_to_remove:
                    g.remove(triple)

                # Execute Semantic Remapping Firewall
                output_file = os.path.join(output_folder, f"{full_row_key}.jsonld")
                g = self.semantic_remapping(g)
                raw_jsonld = g.serialize(format="json-ld", context=context)

                clean_graph = Graph()
                clean_graph.parse(data=raw_jsonld, format='json-ld')
                
                if write_files:
                    clean_graph.serialize(
                        destination=output_file, 
                        format=format, 
                        context=context, 
                        indent=2,
                        auto_compact=True
                    )
                results.append(clean_graph)

            except Exception as e:
                warnings.warn(f"Error processing row {idx} with key {row_key if 'row_key' in locals() else 'N/A'}: {e}")

        return results

    def serialize_bulk(self, 
                      output_path: str, 
                      format = 'json-ld', 
                      row_key_cols: Optional[list[str]] = None, 
                      id_cols: Optional[list[str]] = None,
                      label_pairs: Optional[list[tuple[str, str]]] = None,  
                      license: Optional[str] = None,
                      write_files: Optional[bool] = True) -> Graph:
        """
        Aggregates all row-level RDF graphs into a single master file while preserving the original context.

        This method performs a "Bulk Serialization" by first generating RDF subgraphs for every 
        row in the DataFrame and then merging them into a singular master Graph object. 
        Unlike 'serialize_row', which creates multiple files, this method outputs one 
        unified dataset file, ensuring that the JSON-LD '@context' is applied globally 
        to maintain consistent prefixing (e.g., 'mds:', 'qudt:') across all entries.

        Args:
            output_path (str): The full destination path, including the filename and 
                extension, where the aggregated graph will be saved.
            format (str, optional): The RDF serialization format (e.g., 'json-ld', 
                'turtle', 'xml'). Defaults to 'json-ld'.
            row_key_cols (list[str], optional): Column names used to generate unique 
                row identifiers.
            id_cols (list[str], optional): Column names to be used as entity 
                identifiers (@id) instead of row keys.
            label_pairs (list[tuple[str, str]], optional): A list of 2-tuples (X, Y) 
                where column X represents an entity in the dataframe, and 
                column Y contains the literal text string that should be assigned 
                as its 'rdfs:label'. If a cell in column Y is missing or empty, 
                the label triple for that row is omitted.
            license (str, optional): SPDX license ID or URI to be applied to the 
                triples.
            write_files (bool, optional): Whether to write serialized data to disk. 
                Defaults to True.

        Returns:
            Graph: A single aggregated RDFLib Graph object containing the triples 
                for every row in the dataset.

        Note:
            - This method is highly recommended for creating FAIR-compliant datasets 
              destined for Triple Stores or Graph Databases.
            - It maintains the exact same URI structure and namespace bindings as 
              individual row serializations to ensure interoperability.
            - The output directory is automatically created if it does not exist.
        """
        # 1. Initialize the master graph
        master_graph = Graph()
        metadata_obj = self.metadata_obj
        
        # 2. Extract the original context to ensure consistency
        # This is what keeps your "mds:" and "qudt:" prefixes alive
        context = metadata_obj.metadata_temp.get("@context", {})

        # 3. Generate all row graphs
        # serialize_row to have a 'write_files=False' flag.
        row_graphs = self.serialize_row(
            output_folder=os.path.dirname(output_path),
            format=format,
            row_key_cols=row_key_cols,
            id_cols=id_cols,
            label_pairs=label_pairs,
            license=license,
            write_files = False
        )

        # 4. Merge all triples into the master graph
        for g in row_graphs:
            master_graph += g

        # 5. Save the aggregated file using the original context
        
        if write_files:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            master_graph.serialize(
                destination=output_path,
                format=format,
                context=context,
                indent=2,
                auto_compact=True
            )
            print(f"✅ Bulk file saved at: {output_path}")

        return master_graph



    @classmethod
    def from_rdf_dir(cls, 
                     input_dir: str, 
                     orcid: str = '0000-0000-0000-0000', 
                     metadata_template: Optional[dict] = None,
                     data_relations_dict: Optional[dict] = None,
                     df_name: str = "Imported_RDF_Data",
                     ontology_graph: Optional[Graph] = None,
                     base_uri: str = "https://cwrusdle.bitbucket.io/mds/"):
        """
        Factory method to reconstruct a MatDatSciDf instance and validate semantic integrity 
        from a directory of RDF files.

        This method crawls a directory for supported RDF formats, parses the triples, and 
        reconstructs the tabular data (DataFrame) and metadata (JSON-LD Template). It 
        serves as a data audit pipeline by cross-referencing file-level triples against 
        a master template for unit consistency and a user-provided schema for structural 
        integrity.

        Args:
            input_dir (str): Path to the directory containing RDF files (JSON-LD, Turtle, etc.).
            orcid (str): The ORCID identifier of the user performing the reconstruction.
            data_relations_dict (dict, optional): The expected Subject-Predicate-Object 
                schema to validate against each file. If provided, mismatches are logged.
            df_name (str, optional): Descriptive name for the resulting DataFrame and 
                validation report. Defaults to "Imported_RDF_Data".
            ontology_graph (rdflib.Graph, optional): A reference ontology used to resolve 
                labels and CURIEs during validation.
            base_uri (str, optional): The base URI used for semantic subject identification. 
                Defaults to "https://cwrusdle.bitbucket.io/mds/".

        Returns:
            MatDatSciDf: A fully initialized and validated instance containing the 
                reconstructed dataset and associated semantic logs.

        Reports & Logs:
            - Generates '{df_name}_import_validation.txt' in the input directory.
            - Logs Unit Conflicts: Flagged if a column unit differs from the first 
              encountered definition.
            - Logs Schema Mismatches: Flagged if expected semantic links are missing 
              within individual RDF graphs.

        Note:
            - Supported extensions: .jsonld, .ttl, .nt, .rdf, .xml.
            - Missing data columns in specific files are filled with 'pd.NA' to maintain 
              tabular integrity.
        """
        EXTENSIONS = {
            ".jsonld": "json-ld", ".ttl": "turtle", 
            ".nt": "nt", ".rdf": "xml", ".xml": "xml"
        }
        
        MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
        QUDT = Namespace("http://qudt.org/schema/qudt/")
        UNIT = Namespace("https://qudt.org/vocab/unit/")
        SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

        # Use the passed graph if available, otherwise fallback to class default
        target_onto = ontology_graph if ontology_graph is not None else cls.mds_graph
        
        onto_props = {}
        if target_onto:
            for prop_type, label_type in [(OWL.ObjectProperty, "Object Property"), 
                                          (OWL.DatatypeProperty, "Datatype Property")]:
                for prop in target_onto.subjects(RDF.type, prop_type):
                    label = target_onto.value(prop, RDFS.label)
                    if label:
                        onto_props[str(label)] = (str(prop), label_type)

        expected_metadata = {}

        data_rows = []
        template_items = {}
        
        # Standard context for reconstruction
        reconstructed_context = {
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
            "obo": "http://purl.obolibrary.org/obo/",
            "unit": "https://qudt.org/vocab/unit/"     
        }

        type_mismatches = []
        relations_schema_mismatches = []
        unit_conflicts = []
        parsed_files_count = 0
        template_origins = {}
        if metadata_template and "@graph" in metadata_template:
            for item in metadata_template["@graph"]:
                label = item.get("skos:altLabel")
                if label:
                    # Clean up unit to a simple string/CURIE for comparison
                    u_data = item.get("qudt:hasUnit", {})
                    expected_metadata[label] = {
                        "type": item.get("@type"),
                        "unit": u_data.get("@id") if isinstance(u_data, dict) else u_data
                    }
                    template_origins[label] = "User-provided Template"

        for root, _, files in os.walk(input_dir):
            supported_files = [f for f in files if os.path.splitext(f)[1].lower() in EXTENSIONS]

            for filename in tqdm(supported_files, desc=f"Initializing {df_name}"):
                path = os.path.join(root, filename)
                ext = os.path.splitext(filename)[1].lower()
                
                try:
                    g = Graph()
                    g.parse(path, format=EXTENSIONS[ext])
                    # PRE-BIND NAMESPACES BEFORE PARSING
                    g.bind("mds", MDS)
                    g.bind("qudt", QUDT)
                    g.bind("unit", UNIT)
                    g.bind("skos", SKOS)
                    
                    row = {}
                    # Process every entity that represents a data column
                    for subj in g.subjects(SKOS.altLabel, None):
                        label = str(g.value(subj, SKOS.altLabel)).strip()
                        val = g.value(subj, QUDT.value)
                        unit_node = g.value(subj, QUDT.hasUnit)
                        unit_uri = g.namespace_manager.curie(str(unit_node) if unit_node else "")
                        semantic_type = str(g.value(subj, RDF.type) or "")

                        # 1. Extract Value (Native Python type if Literal, else String/URI)
                        if val is not None:
                            row[label] = val.toPython() if isinstance(val, Literal) else str(val)
                        else:
                            row[label] = pd.NA
                        
                        if label in expected_metadata:
                            expected = expected_metadata[label]
                            
                            # Validate Type
                            if expected["type"] and semantic_type != expected["type"]:
                                type_mismatches.append(f"{filename}: {label} Type is '{semantic_type}' (Expected '{expected['type']}')")
                            
                            # Validate Unit
                            if expected["unit"] and unit_uri != expected["unit"]:
                                unit_conflicts.append(f"{filename}: {label} Unit is '{unit_uri}' (Expected '{expected['unit']}')")

                        # 2. Reconstruct Template Metadata
                        if label not in template_items:
                            if metadata_template:
                                warnings.warn(f"⚠️ Discovery Warning: File {filename} contains column '{label}' not found in provided template.")

                            template_origins[label] = filename
                            template_items[label] = {
                                "@id": semantic_type,
                                "@type": semantic_type,
                                "skos:altLabel": label,
                                "skos:definition": str(g.value(subj, SKOS.definition) or ""),
                                "qudt:hasUnit": {"@id": unit_uri},
                                "mds:hasStudyStage": str(g.value(subj, MDS.hasStudyStage) or "")
                            }

                        else:
                            if not expected_metadata:
                                existing_unit = template_items[label]["qudt:hasUnit"]["@id"]
                                existing_type = template_items[label]["@type"]
                                
                                if unit_uri != existing_unit and unit_uri != "":
                                    unit_conflicts.append(f"{filename}: Unit mismatch for '{label}' vs {template_origins[label]}")
                                if semantic_type != existing_type:
                                    type_mismatches.append(f"{filename}: Type mismatch for '{label}' vs {template_origins[label]}")

                    if data_relations_dict:
                        for prop_key, pairs in data_relations_dict.items():
                            # Determine property URI and type (ObjectProperty vs DatatypeProperty)
                            if prop_key in onto_props:
                                p_uri_str, prop_type = onto_props[prop_key]
                                p_uri = URIRef(p_uri_str)
                            else:
                                p_uri, prop_type = resolve_predicate(prop_key, target_onto)

                            for subj_col, obj_target in pairs:
                                # Check only if subject variable exists in the current file
                                if subj_col in row and row[subj_col] is not pd.NA:
                                    s_uri = URIRef(template_items[subj_col]["@id"])

                                    # --- CASE 1: Object Property (Entity -> Entity) ---
                                    if prop_type == "Object Property" or "ObjectProperty" in str(prop_type):
                                        if obj_target in row and row[obj_target] is not pd.NA:
                                            o_uri = URIRef(template_items[obj_target]["@id"])
                                            if (s_uri, p_uri, o_uri) not in g:
                                                relations_schema_mismatches.append(
                                                    f"{filename}: ObjectProperty Mismatch ({subj_col} -[{prop_key}]-> {obj_target})"
                                                )

                                    # --- CASE 2: Datatype Property (Entity -> Literal Value) ---
                                    elif prop_type == "Datatype Property" or "DatatypeProperty" in str(prop_type):
                                        # Check if s_uri has ANY triple with predicate p_uri in graph g
                                        # or explicitly match against the parsed literal value
                                        val = g.value(s_uri, p_uri)
                                        if val is None:
                                            relations_schema_mismatches.append(
                                                f"{filename}: Missing DatatypeProperty ({subj_col} -[{prop_key}]-> Literal)"
                                            )
                    
                    parsed_files_count += 1

                    row["__source_file__"] = filename
                    data_rows.append(row)

                except Exception as e:
                    print(f"❌ Error parsing {filename}: {e}")

        if len(relations_schema_mismatches) > 0:
            warnings.warn(f"Schema Integrity Warning: {len(relations_schema_mismatches)} mismatches found.")

        if len(unit_conflicts) > 0:
            warnings.warn(f"Unit Conflict Warning: {len(unit_conflicts)} unit conflicts found")
        

        # --- Generate and Save the Report ---
        report_path = os.path.join(input_dir, f"{df_name}_import_validation.txt")
        has_issues = any([type_mismatches, unit_conflicts, relations_schema_mismatches])
        status_str = "⚠️  WARNINGS FOUND" if has_issues else "✅  PASSED"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("=== 🔬 Semantic Data Reconstruction Report ===\n")
            f.write("=" * 60 + "\n")
            f.write(f"Generated:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Dataset Name:   {df_name}\n")
            f.write(f"Validation:     {status_str}\n")
            f.write("-" * 60 + "\n\n")

            f.write("📁 METADATA ORIGINS\n")
            f.write(f"{'Column Label':<25} | {'Defining Source'}\n")
            f.write("-" * 60 + "\n")
            for label, origin in template_origins.items():
                f.write(f"{label:<25} | {origin}\n")
            
            f.write("\n RDF TYPE MISMATCHES ({len(type_mismatches)})\n")
            if not type_mismatches: 
                f.write("  None detected.\n")
            else:
                for err in type_mismatches: 
                    f.write(f"  [!] {err}\n")

            f.write(f"\n📏 UNIT CONFLICTS ({len(unit_conflicts)})\n")
            if not unit_conflicts: 
                f.write("  None detected.\n")
            else:
                for err in unit_conflicts: 
                    f.write(f"  [!] {err}\n")
            
            f.write(f"\n🔗 SCHEMA MISMATCHES ({len(relations_schema_mismatches)})\n")
            if not relations_schema_mismatches: 
                f.write("  None detected.\n")
            else:
                for err in relations_schema_mismatches: 
                    f.write(f"  [!] {err}\n")

        print(f"📄 Import validation report saved to: {report_path}")

        # Final assembly
        df_clean = pd.DataFrame(data_rows)
        
        # Ensure all columns exist in the DF (even if some files missed them)
        for label in template_items.keys():
            if label not in df_clean.columns:
                df_clean[label] = pd.NA

        recon_template = {
            "@context": reconstructed_context,
            "@graph": list(template_items.values())
        }

        if metadata_template:
            return cls(
                df=df_clean,
                metadata_template=metadata_template,
                orcid=orcid,
                data_relations_dict=data_relations_dict,
                df_name=df_name,
                ontology_graph=ontology_graph,
                base_uri=base_uri
            )
        else:
            return cls(
                df=df_clean,
                metadata_template=recon_template,
                orcid=orcid,
                data_relations_dict=data_relations_dict,
                df_name=df_name,
                ontology_graph=ontology_graph,
                base_uri=base_uri
            )

    @classmethod
    def from_jsonld_list(cls, 
                         jsonld_list: List[Union[dict, str]], 
                         orcid: str = '0000-0000-0000-0000', 
                         metadata_template: Optional[dict] = None,
                         data_relations_dict: Optional[dict] = None,
                         df_name: str = "Imported_JSONLD_Data",
                         ontology_graph: Optional[Graph] = None,
                         base_uri: str = "https://cwrusdle.bitbucket.io/mds/"):
        """Factory method to reconstruct a MatDatSciDf instance from in-memory JSON-LD payloads.

        Stages an in-memory collection of JSON-LD dictionaries or JSON-LD serialized strings
        into an isolated temporary directory, delegates execution to `from_rdf_dir` to 
        reconstruct tabular data and execute semantic integrity checks, prints the generated 
        validation audit report to stdout, and cleans up temporary artifacts.

        Args:
            jsonld_list (List[Union[dict, str]]): A list containing in-memory JSON-LD objects, 
                where elements can be Python dictionaries or pre-serialized JSON-LD strings.
            orcid (str, optional): The ORCID identifier of the user performing the 
                reconstruction. Defaults to '0000-0000-0000-0000'.
            metadata_template (dict, optional): A master JSON-LD template dictionary used 
                as a baseline schema for unit, type, and column verification. Defaults to None.
            data_relations_dict (dict, optional): Expected Subject-Predicate-Object schema 
                mapping to validate relational graph integrity across objects. Defaults to None.
            df_name (str, optional): Descriptive name for the resulting DataFrame and 
                validation report logging. Defaults to "Imported_JSONLD_Data".
            ontology_graph (rdflib.Graph, optional): Reference RDF graph used to resolve 
                labels and CURIEs during schema validation. Defaults to None.
            base_uri (str, optional): Base URI used for semantic subject identification. 
                Defaults to "https://cwrusdle.bitbucket.io/mds/".

        Returns:
            MatDatSciDf: A fully initialized and validated MatDatSciDf instance containing 
                the reconstructed dataset, associated metadata template, and semantic logs.

        Raises:
            ValueError: If any item in `jsonld_list` is neither a `dict` nor a `str`.

        Reports & Output:
            - Reads and prints the content of '{df_name}_import_validation.txt' directly to 
              stdout before temporary file teardown.
            - Audits RDF type mismatches, unit conflicts against expected QUDT definitions, 
              and missing relational schema triples.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"📁 Temporary directory is created at: {temp_dir}")
            
            # 1. Stage in-memory JSON-LD items into temporary files
            for idx, item in enumerate(jsonld_list):
                file_path = os.path.join(temp_dir, f"record_{idx}.jsonld")
                
                with open(file_path, "w", encoding="utf-8") as f:
                    if isinstance(item, dict):
                        json.dump(item, f, indent=2)
                    elif isinstance(item, str):
                        f.write(item)
                    else:
                        raise ValueError(f"Invalid payload type at index {idx}: {type(item)}")

            # 2. Delegate to original file-based reconstruction
            instance = cls.from_rdf_dir(
                input_dir=temp_dir,
                orcid=orcid,
                metadata_template=metadata_template,
                data_relations_dict=data_relations_dict,
                df_name=df_name,
                ontology_graph=ontology_graph,
                base_uri=base_uri
            )

            # 3. Read and print the validation report before temp_dir is deleted
            report_path = os.path.join(temp_dir, f"{df_name}_import_validation.txt")
            if os.path.exists(report_path):
                print("\n" + "=" * 60)
                print(f"📋 PRINTING IMPORT VALIDATION REPORT ({df_name})")
                print("=" * 60)
                with open(report_path, "r", encoding="utf-8") as f:
                    print(f.read())
                print("=" * 60 + "\n")

        # temp_dir and its contents (including record_*.jsonld and report) are deleted here
        return instance

    def save_mds_df(self, 
                    output_dir: str, 
                    metadata_in_output_df: bool = False, 
                    formats: list = ["csv", "parquet", "arrow"]):
        """
        Saves the internal DataFrame and associated metadata to the local file system.

        This method supports multi-format export (CSV, Parquet, Arrow). It can also 
        generate a 'semantic' version of the CSV where the first three rows of the 
        file contain the RDF Type, QUDT Unit, and Study Stage for each column, 
        facilitating human readability and FAIR data principles.

        Args:
            output_dir (str): The directory path where files will be stored.
            metadata_obj (Metadata, optional): The Metadata management object. If 
                provided, it will also trigger the saving of the JSON-LD template 
                and match logs.
            metadata_in_output_df (bool, optional): If True, prepends three header 
                rows (Type, Units, Study Stage) to the CSV output. Defaults to False.
            formats (list, optional): A list of strings specifying output formats. 
                Supported: 'csv', 'parquet', 'arrow', 'feather'. 
                Defaults to ["csv", "parquet", "arrow"].

        Note:
            - When 'metadata_in_output_df' is True, only the CSV format will contain 
               the multi-row headers. Parquet and Arrow formats are saved using a 
               'clean' version (data only) to preserve strict schema typing.
            - For Parquet and Arrow exports, all columns are cast to strings to 
               ensure compatibility with mixed-type metadata fields.
            - The method automatically standardizes column order alphabetically.

        Returns:
            None
        """
        os.makedirs(output_dir, exist_ok=True)
        
        df = self.df.copy() # Work on a copy to avoid modifying the instance's df
        output_base_name = self.df_name
        metadata_output_path = os.path.join(output_dir,f"{output_base_name}_template.json")
        matched_output_path = os.path.join(output_dir,f"{output_base_name}_template_matched.log")
        unmatched_output_path = os.path.join(output_dir,f"{output_base_name}_template_unmatched.log")
        relations_output_path = os.path.join(output_dir, f"{output_base_name}_relations")
        
        # 1. Standardize column order: Alphabetical + __source_file__ at the end
        helper_cols = ["__source_file__", "__rowkey__", "__Label__"]
        cols = [col for col in df.columns if col not in helper_cols]
        cols.sort()
        existing_helpers = [h for h in helper_cols if h in df.columns]
        final_cols = cols + existing_helpers
        df = df[final_cols]

        # 2. Prepare the final output DataFrame
        if metadata_in_output_df:
            # Extract metadata from the internal template generator or object
            template_graph = self.metadata_template.get("@graph", [])
            
            # Initialize header dictionaries
            fair_types = {col: "" for col in final_cols}
            units = {col: "" for col in final_cols}
            study_stages = {col: "" for col in final_cols}

            # Map template info to columns based on skos:altLabel
            for item in template_graph:
                label = item.get("skos:altLabel")
                if label in fair_types:
                    fair_types[label] = item.get("@type", "")
                    # Handle qudt:hasUnit being either a string or a dict
                    u = item.get("qudt:hasUnit", "")
                    units[label] = u.get("@id", "") if isinstance(u, dict) else u
                    study_stages[label] = item.get("mds:hasStudyStage", "")

            # Create the 3-row header DataFrame
            header_df = pd.DataFrame([fair_types, units, study_stages])
            
            # Combine headers and data
            df_to_save = pd.concat([header_df, df], ignore_index=True)
            
            # Add the __Label__ column for row identification
            labels = ["Type", "Units", "Study Stage"] + [str(i) for i in range(1, len(df) + 1)]
            # Explicitly convert to Series to resolve type ambiguity
            df_to_save.insert(0, "__Label__", pd.Series(labels))

        else:
            # Just the clean data
            df_to_save = df

        # 3. Handle File Exports
        # Note: We save the 'headered' version to CSV, but usually Parquet/Arrow 
        # should stay 'clean' for better schema compatibility.
        

        self.metadata_obj.save_metadata(
                output_path=metadata_output_path,
                matched_log_path=matched_output_path,
                unmatched_log_path=unmatched_output_path
            )

        self.data_relations.save_relations(
            output_path=relations_output_path
        )

        if "csv" in formats:
            csv_path = os.path.join(output_dir, f"{output_base_name}.csv")
            df_to_save.to_csv(csv_path, index=False)
            
        # Parquet and Arrow/Feather don't handle mixed-type headers well, 
        # so we save the 'clean' df for these.
        if "parquet" in formats:
            pq_path = os.path.join(output_dir, f"{output_base_name}.parquet")
            df.astype(str).to_parquet(pq_path, index=False)
            
        if "arrow" in formats or "feather" in formats:
            ar_path = os.path.join(output_dir, f"{output_base_name}.arrow")
            df.astype(str).to_feather(ar_path)

        print(f"✅ Dataframe '{output_base_name}' saved to {output_dir}")

    def __repr__(self):
        """Provides a summary of the MatDatSciDf object."""
        n_rows = len(self.df)
        n_cols = len(self.df.columns)
        matched = len(self.metadata_template.get("@graph", []))
        
        # Calculate total number of semantic links defined in the DataRelationsDict
        # We check if 'data_rel_obj' exists or pass it as an argument if needed.
        # Assuming you store it in the instance after validation:
        
        # Formatting for the ORCID status
        verification = "✅" if self.orcid_verified else "❌"
        rel_object = self.data_relations

        n_relations = sum(len(pairs) for pairs in rel_object.prop_pair_dict.values())

        return (f"MatDatSciDf: {self.df_name}\n"
                f"---------------------------\n"
                f"Rows:    {n_rows}\n"
                f"Columns: {n_cols}\n"
                f"Metadata matched: {matched} classes\n"
                f"Relations defined: {n_relations} semantic links\n"
                f"Curator ORCID:    {self.orcid} {verification}\n"
                f"Base URI:         {self.base_uri}\n")

    @staticmethod
    def search_license(query: str):
        """
        Searches the SPDX license database for a matching ID or name.
        
        This is a utility method: it can be called as MatDatSciDf.search_license("MIT")
        without initializing the class (i.e., no DataFrame or ORCID required).

        Args:
            query (str): The search term (e.g., 'Creative Commons', 'GPL', 'MIT').
        """
        try:
            # Reusing your utility function that loads the list of license dicts
            spdx_data = load_licenses()
            # The JSON format typically has a "licenses" key containing the list
            licenses = spdx_data.get("licenses", [])
            
            search_term = query.lower()
            results = []

            for lic in licenses:
                lic_id = lic.get("licenseId", "")
                full_name = lic.get("name", "")
                
                # Check for matches in both the short ID and the full Name
                if search_term in lic_id.lower() or search_term in full_name.lower():
                    results.append({
                        "SPDX ID": lic_id,
                        "Full Name": full_name,
                        "OSI Approved": "✅" if lic.get("isOsiApproved") else "❌",
                        "Deprecated": "⚠️" if lic.get("isDeprecatedLicenseId") else "No"
                    })

            if not results:
                print(f"No licenses found matching '{query}'.")
                return

            # Display as a clean table for Jupyter/Terminal readability
            df_results = pd.DataFrame(results)
            print(f"\n--- SPDX License Search Results for '{query}' ---")
            print(df_results.to_string(index=False))
            print("--------------------------------------------------")
            print("Use the 'SPDX ID' string in your serialization methods.")

        except Exception as e:
            # We use a broad catch here only for the UI print, 
            # as network/file issues shouldn't crash the user's session.
            print(f"⚠️ Could not access the license database: {e}")









