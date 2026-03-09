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
from rdflib.namespace import RDF, SKOS, OWL, RDFS, DCTERMS
from urllib.parse import quote
import traceback
import requests
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from typing import Optional
from .utility import (
    load_licenses, 
    hash6, 
    resolve_predicate, 
    write_license_triple, 
    normalize, 
    extract_terms_from_ontology, 
    find_best_match, 
    extract_qudt_units, 
    prompt_for_missing_fields
)
import ast
from tqdm import tqdm

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
        data_relations (MatDatSciDf.DataRelationDict): The internal manager for 
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
                metadata_template: dict,
                data_relations_dict: Optional[dict] = None, 
                orcid: str = "0000-0000-0000-0000", 
                df_name: Optional[str] = None,
                metadata_rows: Optional[bool] = False,
                ontology_graph: Optional[Graph] = None, 
                base_uri="https://cwrusdle.bitbucket.io/mds/"):
        """
        Initializes the MatDatSciDf instance, validates identity, and constructs semantic objects.

        Args:
            df (pd.DataFrame): The source DataFrame containing experimental results.
            metadata_template (dict): The initial JSON-LD dictionary defining column contexts.
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

        self.df = df.iloc[skip_rows:]

        self.metadata_template = metadata_template

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

        if ontology_graph is None:
            if MatDatSciDf.mds_graph is None:
                print("MDS-Onto from source is not available, please parse ontology from a local file")
                user_defined_onto = Graph()
                self.ontology = user_defined_onto
            else:
                self.ontology = MatDatSciDf.mds_graph
        else:
            self.ontology = ontology_graph
        self.base_uri = base_uri

        self.MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
        if data_relations_dict is None:
            data_relations_dict = {}

        self.data_relations = self.DataRelationDict(prop_col_pair_dict=data_relations_dict)
        self.metadata_obj = self.Metadata(metadata_template=self.metadata_template)
        

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
        used in a DataRelationDict to link columns together.
        """

        view_all_props = self.get_relations()
        for key, value in view_all_props.items():
            print(f"{key}: {value}")

    #### DATA RELATION DICTIONARY OBJECT #####

    class DataRelationDict:
        """
        Manages semantic relationships between DataFrame columns for RDF serialization.

        This class stores and organizes mappings that define how columns relate to 
        one another using RDF Object or Datatype properties. These relations are 
        later used by the serializer to generate triples that connect different 
        entities within the same row.

        Attributes:
            prop_pair_dict (dict): A dictionary where keys are property names (URIs or CURIEs) 
                and values are lists of tuples, each containing a (subject_column, object_column) pair.
        """

        def __init__(self, prop_col_pair_dict: dict):
            """
            Initializes the DataRelationDict with optional starting relations.

            Args:
                prop_col_pair_dict (dict): Initial mapping of properties to column pairs. 
                    Example: {"mds:measuredBy": [("Temperature", "Sensor_ID")]}. 
                    Defaults to an empty dictionary.
            """

            self.prop_pair_dict = prop_col_pair_dict


        def add_relation(self, data_relations: dict, ontology_graph: Graph, onto_props: dict):
            """
            Merges new column relationships into the dictionary with ontology validation.

            Args:
                data_relations (dict): Dictionary of {property: [(subj, obj), ...]}.
                ontology_graph (Graph): The RDFLib graph for CURIE expansion.
                onto_props (dict): The result of get_relations() used for label/URI lookups.
            """
            # Create a lookup for all valid URIs currently in the ontology metadata
            valid_uris = {value[0] for value in onto_props.values()}

            for prop_key, pairs_list in data_relations.items():
                resolved_uri = None

                # --- Validation Logic ---
                # 1. Is it a label?
                if prop_key in onto_props:
                    resolved_uri = onto_props[prop_key][0]
                # 2. Is it a full URI?
                elif prop_key in valid_uris:
                    resolved_uri = prop_key
                # 3. Is it a CURIE?
                elif ":" in prop_key:
                    try:
                        expanded = str(ontology_graph.namespace_manager.expand_curie(prop_key))
                        if expanded in valid_uris:
                            resolved_uri = expanded
                    except (ValueError, KeyError):
                        pass

                # If the property is not found, issue a warning but still add it 
                # (allowing for custom properties if the user insists)
                if not resolved_uri:
                    warnings.warn(f"⚠️ Property Warning: '{prop_key}' is not defined in the loaded ontology. "
                                  f"This will result in missing triples during serialization.")

                # --- Merge Logic ---
                if prop_key in self.prop_pair_dict:
                    self.prop_pair_dict[prop_key].extend(pairs_list)
                else:
                    self.prop_pair_dict[prop_key] = pairs_list

            print(f"✅ Integrated {len(data_relations)} property groups into the relation dictionary.")

        def validate_data_relations(self, df: pd.DataFrame, ontology_graph: Graph, onto_props: dict, df_name: Optional[str] = "DataFrame") -> bool:
            """
            Validates the DataRelationDict against the DataFrame and the Ontology.

            This method ensures that:
            1. Every property key used can be resolved (via rdfs:label, CURIE, or full URI).
            2. Every column name paired with a property actually exists in the DataFrame.

            Args:
                df (pd.DataFrame): The DataFrame containing the experimental data.
                df_name (str, Optional): Name of the DataFrame.
                ontology_graph (Graph): The RDFLib Graph object for the ontology (used for CURIE expansion).
                onto_props (dict): The dictionary from MatDatSciDf.get_relations() 
                    mapping labels to (URI, Type).

            Returns:
                bool: True if all relations and columns are valid, False otherwise.
            """
            all_valid = True
            df_columns = set(df.columns)
            
            # Create a lookup for all valid URIs currently in the ontology metadata
            # value[0] is the URI string from our (URI, type) tuple
            valid_uris = {value[0] for value in onto_props.values()}

            print(f"🔍 Starting validation of {len(self.prop_pair_dict)} properties...")

            for prop_key, pairs in self.prop_pair_dict.items():
                resolved_uri = None

                # 1. Resolve Property: Is it an rdfs:label?
                if prop_key in onto_props:
                    resolved_uri = onto_props[prop_key][0]
                
                # 2. Resolve Property: Is it a full URI already?
                elif prop_key in valid_uris:
                    resolved_uri = prop_key
                
                # 3. Resolve Property: Is it a CURIE (e.g., 'mds:measuredBy')?
                elif ":" in prop_key:
                    try:
                        # Attempt to expand the CURIE (e.g., mds -> https://...)
                        expanded = str(ontology_graph.namespace_manager.expand_curie(prop_key))
                        if expanded in valid_uris:
                            resolved_uri = expanded
                    except (ValueError, KeyError):
                        pass

                # Final check for property resolution
                if not resolved_uri:
                    print(f"❌ Property Error: '{prop_key}' is not a valid Label, CURIE, or URI in the ontology.")
                    all_valid = False
                else:
                    # Property is valid; now check the column pairs associated with it
                    for subj_col, obj_col in pairs:
                        if subj_col not in df_columns:
                            print(f"❌ Column Error: Subject '{subj_col}' for property '{prop_key}' not found in {df_name}.")
                            all_valid = False
                        if obj_col not in df_columns:
                            print(f"❌ Column Error: Object '{obj_col}' for property '{prop_key}' not found in {df_name}.")
                            all_valid = False

            if all_valid:
                print("✅ DataRelationDict is valid and ready for serialization.")
            else:
                print("🛑 Validation failed. Please fix the errors listed above.")

            return all_valid

        def print_data_relations(self, 
                                    df: Optional[pd.DataFrame] = None, 
                                    df_name: Optional[str] = "DataFrame",
                                    ontology_graph: Optional[Graph] = None, 
                                    onto_props: Optional[dict] = None):
                """
                        Displays a human-readable summary of column relationships with integrated validation status.

                        This method serves two purposes:
                        1. **Simple Visualization**: If called without arguments, it prints a clean map of the 
                        defined Subject-Predicate-Object relationships.
                        2. **Active Validation**: If a DataFrame and Ontology components are provided, it 
                        performs a "pre-flight check" to verify that every property exists in the ontology 
                        and every column exists in the data.

                        The output uses status symbols:
                        - ✅ : The property or column is valid or validation was skipped.
                        - ❌ [Property Unknown] : The property key could not be resolved as a Label, URI, or CURIE.
                        - ❌ [Col 'Name' missing] : The specified column was not found in the DataFrame headers.

                        Args:
                            df (pd.DataFrame, optional): The DataFrame to validate column names against. 
                                Defaults to None.
                            df_name (str, Optional): Name of the DataFrame.
                            ontology_graph (rdflib.Graph, optional): The RDF graph used to expand and 
                                verify CURIEs (e.g., 'mds:term'). Required if 'onto_props' is provided. 
                                Defaults to None.
                            onto_props (dict, optional): A dictionary of valid ontology properties 
                                (Labels mapped to URIs). Defaults to None.

                        Note:
                            - Validation is case-sensitive for both properties and column names.
                            - If 'onto_props' is provided but 'ontology_graph' is None, CURIE resolution 
                            will be skipped, which may result in false-negative errors for prefixed properties.
                        """
                if not self.prop_pair_dict:
                    print("No relations defined in this DataRelationDict.")
                    return

                # Prepare validation sets
                valid_uris = {v[0] for v in onto_props.values()} if onto_props else set()
                df_cols = set(df.columns) if isinstance(df, pd.DataFrame) else set()

                print("\n--- 🔗 Data Relations Validation Summary ---")
                
                for prop, pairs in self.prop_pair_dict.items():
                    prop_resolved = False
                    
                    # Logic: If we have the tools to validate, then we validate. 
                    # Otherwise, we assume True for display purposes.
                    if onto_props:
                        # 1. Check Label or URI
                        if prop in onto_props or prop in valid_uris:
                            prop_resolved = True
                        # 2. Check CURIE (requires the graph)
                        elif ":" in prop and ontology_graph is not None:
                            try:
                                expanded = str(ontology_graph.namespace_manager.expand_curie(prop))
                                if expanded in valid_uris:
                                    prop_resolved = True
                            except (ValueError, KeyError):
                                pass
                        
                        status_icon = "✅" if prop_resolved else "❌ [Property Unknown]"
                    else:
                        status_icon = ""

                    print(f"Property: {prop} {status_icon}")
                    
                    for subj, obj in pairs:
                        # Validate Columns only if df is provided
                        if isinstance(df, pd.DataFrame):
                            subj_icon = "" if subj in df_cols else f" ❌ [Col '{subj}' not found in {df_name}]"
                            obj_icon = "" if obj in df_cols else f" ❌ [Col '{obj}' not found in {df_name}]"
                        else:
                            subj_icon = ""
                            obj_icon = ""
                        
                        print(f"  └─ {subj}{subj_icon} ──▶ {obj}{obj_icon}")
                print("-------------------------------------------\n")

        def save_relations(self, output_path: str):
            """
            Exports the semantic mapping to both JSON (machine-readable) 
            and TXT (human-readable) formats.
            
            Args:
                output_path (str): The destination file path (extension is ignored).
            """
            if not self.prop_pair_dict:
                print("⚠️ No relations defined to save.")
                return

            # Strip extension and prepare directory
            base_path = os.path.splitext(output_path)[0]
            os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)

            # 1. Save JSON (The Machine Source)
            with open(f"{base_path}.json", 'w', encoding='utf-8') as f:
                json.dump(self.prop_pair_dict, f, indent=4)
            
            # 2. Save Text Report (The Human Documentation)
            with open(f"{base_path}.txt", 'w', encoding='utf-8') as f:
                f.write("--- 🔗 Data Relations Mapping ---\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Property Groups: {len(self.prop_pair_dict)}\n")
                f.write("-" * 50 + "\n\n")
                
                for prop, pairs in self.prop_pair_dict.items():
                    f.write(f"Property: {prop}\n")
                    for subj, obj in pairs:
                        f.write(f"  └─ {subj} ──▶ {obj}\n")
                    f.write("\n")
            
            print(f"✅ Relations saved to {base_path}.json and {base_path}.txt")

    ### DATA RELATIONS DICT WRAPPER ###
    
    def add_relation(self, data_relations: dict):
        data_rel_obj = self.data_relations
        onto_graph = self.ontology
        onto_props = self.get_relations()
        data_rel_obj.add_relation(data_relations=data_relations, 
                                ontology_graph=onto_graph, 
                                onto_props=onto_props)

    def validate_data_relations(self):
        """Wrapper to validate relations using the instance's own data and ontology."""
        onto_metadata = self.get_relations()
        data_rel_obj = self.data_relations
        return data_rel_obj.validate_data_relations(self.df, self.ontology, onto_metadata)

    def view_data_relations(self):
        """
        Displays a visual validation report for the provided DataRelationDict.
        """
        self.data_relations.print_data_relations(
            df=self.df, 
            df_name = self.df_name,
            ontology_graph=self.ontology, 
            onto_props=self.get_relations()
        )


    #### METADATA OBJECT ####

    class Metadata:
        """
        Manages semantic metadata and synchronization between JSON-LD templates and RDF graphs.

        This class acts as a specialized container for experimental metadata. It maintains 
        a 'source of truth' using an RDFLib Graph to ensure semantic consistency, while 
        providing a standard dictionary interface for JSON-LD serialization. It also 
        tracks the success of metadata mapping through matched and unmatched logs.

        Attributes:
            metadata_temp (dict): The JSON-LD representation of the metadata template, 
                including @context and @graph.
            matched_log (list): A historical record of columns successfully mapped to 
                ontology terms during the initialization process.
            unmatched_log (list): A record of columns that failed to find an automated 
                match in the reference ontology.
            template_graph (rdflib.Graph): The internal RDFLib Graph used for complex 
                updates, validation, and semantic querying.
            MDS (rdflib.Namespace): Namespace for Materials Data Science ontology terms.
            QUDT (rdflib.Namespace): Namespace for Quantities, Units, Dimensions, and Types.
            UNIT (rdflib.Namespace): Namespace for QUDT unit individuals.
        """

        def __init__(self, metadata_template, matched_log: Optional[list] = None, unmatched_log: Optional[list] = None):
            """
            Initializes the Metadata manager and parses the template into an RDF graph.

            Args:
                metadata_template (dict): The initial JSON-LD dictionary structure.
                matched_log (list, optional): Pre-existing log of successful matches. 
                    Defaults to an empty list.
                unmatched_log (list, optional): Pre-existing log of failed matches. 
                    Defaults to an empty list.
            """

            self.metadata_temp = metadata_template
            self.matched_log = matched_log if matched_log is not None else []
            self.unmatched_log = unmatched_log if unmatched_log is not None else []
            self.template_graph = Graph()
            self.template_graph.parse(data=json.dumps(metadata_template), format="json-ld")
            self.MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
            self.QUDT = Namespace("http://qudt.org/schema/qudt/")
            self.UNIT = Namespace("https://qudt.org/vocab/unit/")
            self.template_graph.bind("unit", self.UNIT)
            self.template_graph.bind("skos", SKOS)
            self.template_graph.bind("mds", self.MDS)
            self.template_graph.bind("qudt", self.QUDT)


        def save_metadata(self, output_path: str, matched_log_path: Optional[str] = None, unmatched_log_path: Optional[str] = None):
            """
            Exports the synchronized metadata template and import logs to the file system.

            This method performs three primary tasks:
            1. Serializes the current JSON-LD metadata template (the source of truth) to a file.
            2. Optionally exports a log of all columns successfully matched during initialization.
            3. Optionally exports a deduplicated log of columns that were not found in the RDF source.

            Args:
                output_path (str): File path where the JSON-LD metadata template will be saved.
                matched_log_path (str, optional): File path to save the list of successfully 
                    matched columns. If None, no log is created.
                unmatched_log_path (str, optional): File path to save the unique list of 
                    columns missing RDF metadata. If None, no log is created.

            Note:
                This method automatically creates any missing parent directories for the 
                provided file paths to prevent 'FileNotFoundError'.

            Returns:
                None
            """
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, "w") as f:
                json.dump(self.metadata_temp, f, indent=2)
            if isinstance(matched_log_path, str):
                os.makedirs(os.path.dirname(matched_log_path), exist_ok=True)
                 # Write matched log
                with open(matched_log_path, "w") as f:
                    f.write("\n".join(self.matched_log))
            if isinstance(unmatched_log_path, str):
                # Write unmatched log (remove duplicates with set)
                os.makedirs(os.path.dirname(unmatched_log_path), exist_ok=True)
                with open(unmatched_log_path, "w") as f:
                    f.write("\n".join(sorted(set(self.unmatched_log)))) 

        ##### TEMPLATE MANIPULATION ####

        def update_template(self, col_name: str, field: str, value: str):
            """
            Updates a specific metadata property for a column within the RDF graph.

            This method identifies a column by its 'skos:altLabel', modifies the 
            corresponding RDF triple in the internal graph using the appropriate 
            namespace (MDS, QUDT, etc.), and synchronizes the change back to the 
            JSON-LD dictionary.

            Args:
                col_name (str): The column name to update (matches the 'skos:altLabel').
                field (str): The shorthand for the property to change. Must be one of:
                    - 'definition': Updates the 'skos:definition' (Text).
                    - 'unit': Updates 'qudt:hasUnit' (URI). Handles 'unit:UNIT' shorthand.
                    - 'type': Updates 'rdf:type' (URI/Class).
                    - 'stage': Updates 'mds:hasStudyStage' (Text/URI).
                    - 'note': Updates 'skos:note' (Text).
                value (str): The new value to assign to the field. 

            Note:
                This method uses RDFLib's `.set()` logic, meaning it will overwrite 
                any existing value for the specified field to maintain a single 
                source of truth for that property.

            Returns:
                None
            """
            # 1. Map the string 'field' to actual RDF Predicates
            field_map = {
                    "definition": SKOS.definition,
                    "unit": self.QUDT.hasUnit,
                    "type": RDF.type,
                    "stage": self.MDS.hasStudyStage,
                    "note": SKOS.note
                }

                # 2. Find the subject (the node) that has the altLabel matching col_name
            subject = self.template_graph.value(predicate=SKOS.altLabel, object=Literal(col_name))

            if subject and field in field_map:
                predicate = field_map[field]

                # 3. Determine the correct RDF Object type based on the field
                if field == "unit":
                    # Ensure unit is a URI. If it's just 'KM', make it 'unit:KM'
                    unit_uri = value if ":" in value or value.startswith("http") else f"unit:{value}"
                    # If using the UNIT namespace, we can expand it
                    if unit_uri.startswith("unit:"):
                        new_obj = self.UNIT[unit_uri.split(":")[1]]
                    else:
                        new_obj = URIRef(unit_uri)
                
                elif field == "type":
                    # Types are usually URIs in your MDS namespace
                    new_obj = self.MDS[value] if ":" not in value else URIRef(value)
                
                else:
                    # Definitions and notes are plain text Literals
                    new_obj = Literal(value)

                # 4. Use .set() to replace the old value with the new one
                self.template_graph.set((subject, predicate, new_obj))
                
                # 5. Re-sync the dictionary for the save_metadata method
                # We use a frame or context to keep the JSON-LD structure clean
                context = self.metadata_temp.get("@context", {})
                updated_json = self.template_graph.serialize(format="json-ld", context=context)
                self.metadata_temp = json.loads(updated_json)

                print(f"✅ Successfully updated {field} for '{col_name}'.")
            
            elif not subject:
                print(f"⚠️ Column '{col_name}' not found in the graph.")
            else:
                print(f"⚠️ Field '{field}' is not a recognized field. Try: {list(field_map.keys())}")


        def add_column_metadata(self, col_name: str, rdf_type: str, unit: str = "UNITLESS", definition: str = "No definition provided", study_stage: str = "UNK"):
            """
            Manually adds a new column definition to the metadata template.
            
            This is useful for fixing [UNDEFINED COLUMNS] identified during validation.
            
            Args:
                col_name (str): The column header from the DataFrame.
                rdf_type (str): The MDS property or class (e.g., 'mds:Temperature').
                unit (str): The QUDT unit shorthand (e.g., 'DEG_C'). Defaults to 'UNITLESS'.
                definition (str): A human-readable description of the data.
                study_stage (str): The stage of the study (e.g., 'Synthesis', 'Result').
            """
            # 1. Check if it already exists to avoid duplicates
            existing = self.template_graph.value(predicate=SKOS.altLabel, object=Literal(col_name))
            if existing:
                print(f"⚠️ Metadata for '{col_name}' already exists. Use update_template instead.")
                return

            # 2. Create the internal entry structure
            # We follow the exact format used in your template_generator
            entry = {
                "@id": rdf_type if ":" in rdf_type else f"mds:{rdf_type}",
                "@type": rdf_type if ":" in rdf_type else f"mds:{rdf_type}",
                "skos:altLabel": col_name,
                "skos:definition": definition,
                "qudt:hasUnit": {"@id": f"unit:{unit}"},
                "prov:generatedAtTime": {
                    "@value": datetime.now(timezone.utc).isoformat() + "Z",
                    "@type": "xsd:dateTime"
                },
                "mds:hasStudyStage": study_stage
            }

            # 3. Add to the graph and re-sync the dictionary
            # Adding via serialization ensures all bindings and context remain consistent
            self.template_graph.parse(data=json.dumps(entry), format="json-ld")
            
            context = self.metadata_temp.get("@context", {})
            updated_json = self.template_graph.serialize(format="json-ld", context=context)
            self.metadata_temp = json.loads(updated_json)

            print(f"✅ Successfully added metadata for new column: '{col_name}'.")
        
        def print_template(self, format: str = "table"):
            """
            Prints the current metadata template.
            :param format: 'table' for a summarized DataFrame view, 
                        'json' for the raw JSON-LD structure.
            """
            if format.lower() == "json":
                print(json.dumps(self.metadata_temp, indent=2))
            
            elif format.lower() == "table":
                graph_data = self.metadata_temp.get("@graph", [])
                if not graph_data:
                    print("The template graph is currently empty.")
                    return

                # Extracting key info into a list of dicts for the DataFrame
                summary_list = []
                for item in graph_data:
                    summary_list.append({
                        "Label": item.get("skos:altLabel", "N/A"),
                        "Type": item.get("@type", "N/A"),
                        "Unit": item.get("qudt:hasUnit", {}).get("@id", "None"),
                        "Definition": item.get("skos:definition", "")[:50] + "..." 
                                    if len(item.get("skos:definition", "")) > 50 
                                    else item.get("skos:definition", ""),
                        "Study Stage": item.get("mds:hasStudyStage", "N/A")
                    })
                
                df_summary = pd.DataFrame(summary_list)
                
                print("\n--- Metadata Template Summary ---")
                # If in a Jupyter Notebook, this renders as a nice HTML table
                # If in a terminal, it prints a clean text table
                print(df_summary.to_string(index=False))
                print("------------------------------------------\n")
            
            else:
                print(f"Unknown format '{format}'. Use 'table' or 'json'.")

    #### METADATA OBJECT WRAPPERS ####
    def update_metadata(self, col_name: str, field: str, value: str):
        """
        Wrapper to update a metadata property (unit, type, definition, etc.) 
        for a specific column.
        """
        self.metadata_obj.update_template(col_name, field, value)
        # Update the local dictionary reference to keep them in sync
        self.metadata_template = self.metadata_obj.metadata_temp

    def add_column_metadata(self, col_name: str, rdf_type: str, unit: str = "UNITLESS", 
                            definition: str = "No definition provided", study_stage: str = "UNK"):
        """
        Top-level API to manually define semantic metadata for a new column.
        Useful for defining columns found in 'Discovery Warning' reports.
        """
        self.metadata_obj.add_column_metadata(col_name, rdf_type, unit, definition, study_stage)
        self.metadata_template = self.metadata_obj.metadata_temp

    def view_metadata(self, format: str = "table"):
        """
        Wrapper to print the current metadata template as a 
        formatted table or raw JSON-LD.
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
            "cco": "https://www.commoncoreontologies.org/"      
        },
        "@graph": []
        }   

        units = extract_qudt_units()

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
                    #if term in mds ontology, get study stage and def from ontologyt
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
                    evaluated = ast.literal_eval(un)
                    
                    # Step 2: Extract the string based on type
                    if isinstance(evaluated, dict):
                        # It's a dict, get the ID value
                        target_str = evaluated.get('@id', "")
                    else:
                        # It's already a string (like "unit:UNIT")
                        target_str = str(evaluated)

                    # Step 3: Split and safely get the second part
                    if ":" in target_str:
                        un = target_str.split(":")[1]
                    else:
                        un = target_str # Fallback if no colon exists

                except (ValueError, SyntaxError, IndexError) as e:
                    print(f"Parsing error for value '{un}': {e}")
                    un = "UNITLESS"
                
            if match:
                matched_log.append(f"{col} => {iri_fragment}")

            else:
                unmatched_log.append(col)


            
            if not skip_prompts:
                unit, study, notes = prompt_for_missing_fields(iri_fragment, un, study_stage, ontology_graph, units)
            else:
                unit = "UNITLESS"
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

    def serialize_row(self, 
                    output_folder: str, 
                    format = 'json-ld', 
                    row_key_cols: Optional[list[str]] = None, 
                    id_cols: Optional[list[str]] = None, 
                    license: Optional[str]= None,
                    write_files: Optional[bool] = True) -> list[Graph]:

        """
        Serializes each row of the DataFrame into individual RDF files based on the provided metadata template.

        This method iterates through the DataFrame, generates a unique row key (using either 
        specified columns or a hash of study-stage data), and maps CSV values to RDF subjects 
        defined in the Metadata object. It also handles object and datatype property relationships 
        defined in the data_relation_dict.

        Args:
            output_folder (str): The directory where the serialized RDF files will be saved.
            format (str, optional): The RDF serialization format (e.g., 'json-ld', 'turtle', 'xml'). 
                Defaults to 'json-ld'.
            row_key_cols (list[str], optional): Specific column names to use for generating 
                the unique row identifier. If None, a hash-based key is generated.
            id_cols (list[str], optional): Column names that should be used as part of the 
                entity's URI identifier (@id) instead of the generic row key.
            license (str, optional): An SPDX license ID (e.g., 'MIT', 'CC0-1.0') or a full 
                URI. If None, defaults to CC0-1.0.
            write_files (bool, optional): Whether or not to write the serialized data to disk. 
                Defaults to True

        Raises:
            ValueError: If a provided license ID is not found in the SPDX list or if 
                required metadata (like 'skos:altLabel') is missing from the template.

        Returns:
            List[Graph]: A list of RDFLib Graph objects, one for each successfully 
                serialized row.
        
        Note:
            - Creates the `output_folder` if it does not exist.
            - Writes multiple files to the `output_folder` named with a pattern of 
                '{random_suffix}-{row_key}.jsonld'.
        """

        df = self.df
        orcid =  self.orcid
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
            os.makedirs(os.path.dirname(output_folder), exist_ok=True)

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
            "": "UNK"}

        rowpredicate = URIRef("https://cwrusdle.bitbucket.io/mds/row")
        row_key = ""

        #check license
        if(not license):
            license_uri = URIRef("https://spdx.org/licenses/CC0-1.0.html")
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
            license_uri = URIRef(license_uri)
            write_license_triple(output_folder, base_uri, license_uri)

        else:
            # Full URI provided; assume it's valid
            license_uri = URIRef(license)
            
            write_license_triple(output_folder, base_uri, license_uri)


        for idx, row in df.iterrows():

            try:

                # Deep copy the template and assign @id
                template_copy = copy.deepcopy(graph_template)
                subject_lookup = {}  # Maps skos:altLabel → generated @id

                # generate row key
                c = [item["skos:altLabel"] for item in template_copy ]
                if(row_key_cols is None or not any(x in c for x in row_key_cols) ):
                    keys = {}
                    for item in template_copy:
                        if "skos:altLabel" not in item or not item["skos:altLabel"]:
                            raise ValueError("Missing skos:altLabel in template")
                        col = item["skos:altLabel"]
                        studystage = item["mds:hasStudyStage"]
                        val = df.at[idx,col]

                        if studystage not in keys:
                            keys[studystage] = [val]
                        else:
                            keys[studystage].append(val)
                    row_key = ""
                    for key in keys:
                        try:
                            num = str(hash6("".join([str(x) for x in keys[key] if not pd.isna(x)])))
                            row_key = row_key + sskey[key] + num + "_"
                        except Exception:
                            traceback.print_exc()
                else:
                    row_key = ""
                    for x in set(c) & set(row_key_cols):
                        row_key = row_key + str(df.at[idx,x]).strip() + "_"
                
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                full_row_key = f"{row_key}{orcid}-{timestamp}"
                full_row_key = full_row_key.replace(" ", "")
                

                for item in template_copy: #prepare all fields
                    if "@type" not in item or not item["@type"]:
                        warnings.warn(f"Missing or empty @type in template item: {item}")
                        continue
                    if "skos:altLabel" not in item or not item["skos:altLabel"]:
                        raise ValueError("Missing skos:altLabel in template")

                    prefix, localname = item["@type"].split(":")
                    if id_cols is not None and item["skos:altLabel"] in id_cols:
                        raw_identifier = row.get(item["skos:altLabel"])
                        if not raw_identifier:
                            raise ValueError(f"Cannot find entity identifier in row {idx}")
                        entity_identifier = normalize(re.sub(r'[^a-zA-Z0-9_\-\.]', '', raw_identifier))
                        subject_uri = self.MDS[f"{localname}.{entity_identifier}"]
                    else:
                        subject_uri = self.MDS[f"{localname}.{row_key[:-1]}" if prefix else f"{localname}.{row_key[:-1]}"]
                    item["@id"] = URIRef(subject_uri)
                    subject_lookup[item["skos:altLabel"]] = URIRef(subject_uri)

                    if "prov:generatedAtTime" in item:
                        item["prov:generatedAtTime"]["@value"] = datetime.now(timezone.utc).isoformat() + "Z"

                    if "qudt:hasUnit" in item and not item["qudt:hasUnit"].get("@id"):
                        del item["qudt:hasUnit"]
                    if "qudt:hasQuantityKind" in item and not item["qudt:hasQuantityKind"].get("@id"):
                        del item["qudt:hasQuantityKind"]

                jsonld_data = {
                    "@context": context,
                    "@graph": template_copy
                }

                #convert to rdf graph
                g = Graph(identifier=URIRef(f"{base_uri}{full_row_key}{idx}"))
                g.parse(data=json.dumps(jsonld_data), format="json-ld")
                QUDT = Namespace("http://qudt.org/schema/qudt/")
                MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
                g.bind("mds", MDS)
                g.bind("qudt", QUDT)
                g.bind("dcterms", DCTERMS)


                #separate jsonld not needed
                #write_license_triple(output_folder, base_uri, license_uri)

                #add in triples from csv values
                for alt_label, subj_uri in subject_lookup.items():
                    if alt_label in row:
                        g.remove((subj_uri, QUDT.value, None))
                        if pd.notna(row[alt_label]) and row[alt_label] != "":
                            data_value = row[alt_label]
                            if hasattr(data_value, 'item'):
                                data_value = data_value.item()
                            g.add((subj_uri, QUDT.value, Literal(data_value, datatype=XSD.string)))
                        else:
                            print(f"Skipping NA value for {alt_label} on row {idx} with row key {row_key}")
                        g.add((subj_uri, rowpredicate, Literal(row_key[:-1]) ))
                        g.add((subj_uri, DCTERMS.license, license_uri))
                        curator_uri = URIRef(f"https://orcid.org/{self.orcid}")
                        g.add((subj_uri, DCTERMS.creator, curator_uri))
                        if not self.orcid_verified:
                            g.add((subj_uri, SKOS.note, Literal("Caution: Data curator ORCID was not verified at time of serialization.")))


                # Add object/datatype properties if given
                if prop_column_pair_dict:
                    for key, column_pair_list in prop_column_pair_dict.items():
                        # attempt to resolve via iri, then curie, then skip
                        prop_uri, prop_type = resolve_predicate(key, ontology_graph)

                        if prop_uri is None:
                            #below attempts to create pred_uri based on the prop_metadata_dict
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
                            alt_label = subj_col
                            subj_uri = subject_lookup.get(alt_label)
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
                
                triples_to_remove = []
                for s, p, o in g:
                    if p == QUDT.value and len(str(o).strip()) == 0:
                        if len(str(o).strip()) == 0:
                            print(f"debug removing empty {s}, {p}, {o}")
                            triples_to_remove.append((s, p, o))

                for triple in triples_to_remove:
                    g.remove(triple)

                # Save the RDF graph to file
                random_suffix = ''.join(random.choices(string.ascii_lowercase, k=2))
                output_file = os.path.join(output_folder, f"{random_suffix}-{full_row_key}.jsonld")
                # g.serialize(destination=output_file, format="json-ld", context=context, indent=2, auto_compact=True, encoding='utf-8')
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
                      license: Optional[str] = None,
                      write_files: Optional[bool] = True) -> Graph:
        """
        Aggregates all row-level RDF graphs into a single master file while preserving the original context.

        This method performs a "Deep Serialization" by first generating RDF subgraphs for every 
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
                     orcid: str, 
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
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "prov": "http://www.w3.org/ns/prov#",
            "dcterms": "http://purl.org/dc/terms/",
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
                                "@id": str(subj),
                                "@type": str(g.value(subj, RDF.type) or ""),
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
                            # Check if key is a label in our pre-calculated onto_props
                            if prop_key in onto_props:
                                p_uri = URIRef(onto_props[prop_key][0])
                            else:
                                # Fallback to your utility function
                                p_uri, _ = resolve_predicate(prop_key, target_onto)

                            for subj_col, obj_col in pairs:
                                if subj_col in template_items and obj_col in template_items:
                                    s_uri = URIRef(template_items[subj_col]["@id"])
                                    o_uri = URIRef(template_items[obj_col]["@id"])

                                    if (s_uri, p_uri, o_uri) not in g:
                                        relations_schema_mismatches.append(f"{filename}: {subj_col} -> {obj_col}")
                    
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

    def save_mds_df(self, output_dir: str, metadata_in_output_df: bool = False, formats: list = ["csv", "parquet", "arrow"]):
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
        
        # Calculate total number of semantic links defined in the DataRelationDict
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









