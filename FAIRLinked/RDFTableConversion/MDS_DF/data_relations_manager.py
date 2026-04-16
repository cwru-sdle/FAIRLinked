import os
import json
import warnings
from datetime import datetime
import pandas as pd
from rdflib import Graph
from typing import Optional


#### DATA RELATIONS DICTIONARY OBJECT #####

class DataRelationsDict:
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
            Initializes the DataRelationsDict with optional starting relations.

            Args:
                prop_col_pair_dict (dict): Initial mapping of properties to column pairs. 
                    Example: {"mds:measuredBy": [("Temperature", "Sensor_ID")]}. 
                    Defaults to an empty dictionary.
            """

            self.prop_pair_dict = prop_col_pair_dict


        def add_relations(self, data_relations: dict, ontology_graph: Graph, onto_props: dict):
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

        def delete_relation(self, prop_key: str, pair: Optional[tuple] = None):
            """
            Removes semantic relationships from the dictionary.

            Args:
                prop_key (str): The property label, CURIE, or URI identifying the group.
                pair (tuple, optional): A specific (subject_column, object_column) tuple 
                    to remove. If None, the entire property group is deleted.
            """
            if prop_key not in self.prop_pair_dict:
                print(f"⚠️ Property '{prop_key}' not found in the current relations.")
                return

            if pair is None:
                # 1. Delete the entire property group
                del self.prop_pair_dict[prop_key]
                print(f"✅ Successfully deleted all relations for property: '{prop_key}'.")
            else:
                # 2. Delete a specific (subj, obj) pair
                try:
                    self.prop_pair_dict[prop_key].remove(pair)
                    print(f"✅ Successfully deleted pair {pair} from property: '{prop_key}'.")
                    
                    # Clean up the key if the list is now empty
                    if not self.prop_pair_dict[prop_key]:
                        del self.prop_pair_dict[prop_key]
                        
                except ValueError:
                    print(f"⚠️ Pair {pair} not found under property '{prop_key}'.")

        def validate_data_relations(self, 
                                    df: pd.DataFrame, 
                                    ontology_graph: Graph, 
                                    onto_props: dict, 
                                    df_name: Optional[str] = "DataFrame") -> bool:
            """
            Validates the DataRelationsDict against the DataFrame and the Ontology.

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
                print("✅ DataRelationsDict is valid and ready for serialization.")
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
                    print("No relations defined in this DataRelationsDict.")
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