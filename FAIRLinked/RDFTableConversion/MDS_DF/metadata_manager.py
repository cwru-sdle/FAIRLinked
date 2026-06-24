import os
import json
from datetime import datetime, timezone
import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, SKOS
from typing import Optional

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

        def __init__(self, 
                    metadata_template: dict, 
                    matched_log: Optional[list] = None, 
                    unmatched_log: Optional[list] = None):
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


        def _normalize_graph_structure(self, data: any) -> dict:
            """
            Ensure the serialized JSON-LD always has a '@graph' list.
            """
            # Fix 1: Handle if data is already a list (flattened JSON-LD)
            if isinstance(data, list):
                return {
                    "@context": self.metadata_temp.get("@context", {}),
                    "@graph": data
                }

            # Fix 2: Handle if data is a dict but missing '@graph'
            if isinstance(data, dict) and "@graph" not in data:
                # Use a safe pop that only runs if data is a dict
                context = data.pop("@context") if "@context" in data else {}
                node = {k: v for k, v in data.items()}
                return {"@context": context, "@graph": [node]}
                
            return data


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
        def update_bulk(self, metadata_template: dict):
            """
            Merges an external metadata template into the current instance.
            Iterates through the '@graph' and decides whether to update existing 
            columns or add new ones.
            """
            # 1. Extract the graph list from the JSON-LD structure
            # This handles both flattened and framed JSON-LD formats
            graph_entries = metadata_template.get('@graph', [])

            if not graph_entries:
                print("⚠️ No valid metadata entries found in the provided template.")
                return

            updates_count = 0
            adds_count = 0

            for entry in graph_entries:
                # Identify the column name (the unique key for our mapping)
                col_name = entry.get("skos:altLabel")
                if not col_name:
                    continue

                # Check if this column already exists in our current graph
                existing_subject = self.template_graph.value(predicate=SKOS.altLabel, object=Literal(col_name))

                if existing_subject:
                    # OPTION A: Column exists -> Update specific fields
                    # We map the JSON keys to the 'field' shorthand used in update_template
                    field_mapping = {
                        "skos:definition": "definition",
                        "qudt:hasUnit": "unit",
                        "rdf:type": "type",
                        "@type": "type",
                        "mds:hasStudyStage": "stage",
                        "skos:note": "note"
                    }

                    for json_key, field_shorthand in field_mapping.items():
                        value = entry.get(json_key)
                        if value:
                            # Handle nested @id structures (like in units)
                            if isinstance(value, dict) and "@id" in value:
                                value = value["@id"]
                            
                            self.update_template(col_name, field_shorthand, str(value))
                    updates_count += 1
                
                else:
                    # OPTION B: Column is new -> Add it to the graph
                    rdf_type = entry.get("@type") or "cco:ont00000958"
                    unit_info = entry.get("qudt:hasUnit", "unit:UNITLESS")
                    unit = unit_info["@id"] if isinstance(unit_info, dict) else unit_info
                    
                    self.add_column_metadata(
                        col_name=col_name,
                        rdf_type=str(rdf_type),
                        unit=str(unit).replace("unit:", ""), # Strip prefix for the function
                        definition=entry.get("skos:definition", "Definition not available"),
                        study_stage=entry.get("mds:hasStudyStage", "UNK")
                    )
                    adds_count += 1

            print(f"📊 Bulk Update Summary: {updates_count} columns updated, {adds_count} columns added.")


        def update_template(self, col_name: str, field: str, value: str):
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
            # 1. Map shorthands to both JSON keys and RDF Predicates
            mapping = {
                "definition": ("skos:definition", SKOS.definition),
                "unit": ("qudt:hasUnit", self.QUDT.hasUnit),
                "type": ("@type", RDF.type),
                "stage": ("mds:hasStudyStage", self.MDS.hasStudyStage),
                "note": ("skos:note", SKOS.note)
            }

            if field not in mapping:
                print(f"⚠️ Field '{field}' is not recognized.")
                return

            json_key, predicate = mapping[field]

            # --- PART A: Update self.metadata_temp (The JSON source) ---
            # This is what you were doing; it works because of Python's object referencing
            graph_list = self.metadata_temp.get("@graph", [])
            for item in graph_list:
                if item.get("skos:altLabel") == col_name:
                    if field == "unit":
                        # Standardize to a dict structure for QUDT
                        item[json_key] = {"@id": f"unit:{value}" if ":" not in value else value}
                    else:
                        item[json_key] = value
                    break

            # --- PART B: Update self.template_graph (The RDF source) ---
            # This ensures serialize_row and other graph functions stay in sync
            subject = self.template_graph.value(predicate=SKOS.altLabel, object=Literal(col_name))
            
            if subject:
                # Determine the correct RDF Object type
                if field == "unit":
                    unit_uri = value if ":" in value else f"unit:{value}"
                    new_obj = self.UNIT[unit_uri.split(":")[1]] if "unit:" in unit_uri else URIRef(unit_uri)
                elif field == "type":
                    new_obj = self.MDS[value] if ":" not in value else URIRef(value)
                else:
                    new_obj = Literal(value)

                # Overwrite the triple in the graph
                self.template_graph.set((subject, predicate, new_obj))
                print(f"✅ Synchronized {field} for '{col_name}'.")
            else:
                print(f"⚠️ Warning: '{col_name}' updated in JSON but not found in RDF Graph.")


        def add_column_metadata(self, col_name: str, rdf_type: str, unit: str = "UNITLESS", 
                                definition: str = "Definition not available", study_stage: str = "UNKNOWN"):

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
            # 1. Direct JSON check
            graph = self.metadata_temp.get("@graph", [])
            if any(item.get("skos:altLabel") == col_name for item in graph):
                return

            # 2. Build a clean Python Dict (No RDFLib objects here)
            entry = {
                "@id": rdf_type if ":" in rdf_type else f"mds:{rdf_type}",
                "@type": rdf_type if ":" in rdf_type else f"mds:{rdf_type}",
                "skos:altLabel": col_name,
                "skos:definition": definition,
                "qudt:hasUnit": {"@id": f"unit:{unit}"},
                "prov:generatedAtTime": datetime.now(timezone.utc).isoformat() + "Z",
                "mds:hasStudyStage": study_stage
            }

            # 3. Direct append to the list that serialize_row uses
            graph.append(entry)
            self.metadata_temp["@graph"] = graph
            
            # Optional: If you use the internal graph for OTHER things, 
            # parse ONLY this new entry to keep it in sync
            self.template_graph.parse(data=json.dumps(entry), format="json-ld")

        def delete_column_metadata(self, col_name: str):
            """
            Removes all metadata associated with a specific column from both 
            the internal JSON template and the RDF graph.
            """
            # 1. Direct JSON Manipulation (The "Clean" way)
            # We filter the @graph list directly to avoid RDFLib's serialization noise
            original_graph = self.metadata_temp.get("@graph", [])
            new_graph = [item for item in original_graph if item.get("skos:altLabel") != col_name]

            if len(new_graph) < len(original_graph):
                # 2. Update the JSON template used by serialize_row()
                self.metadata_temp["@graph"] = new_graph

                # 3. Keep the RDF graph in sync
                # We find the node and remove its triples so the graph remains accurate
                subject = self.template_graph.value(predicate=SKOS.altLabel, object=Literal(col_name))
                if subject:
                    self.template_graph.remove((subject, None, None))
                
                print(f"✅ Successfully deleted metadata for column: '{col_name}'.")
            else:
                print(f"⚠️ Column '{col_name}' not found in the metadata.")
        
        def print_template(self, format: str = "table"):
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
                            # Handle qudt:hasUnit which might be a dict, a list, or missing
                            unit_raw = item.get("qudt:hasUnit", "None")
                            
                            # If it's a list, take the first entry
                            if isinstance(unit_raw, list) and len(unit_raw) > 0:
                                unit_val = unit_raw[0].get("@id", "None") if isinstance(unit_raw[0], dict) else str(unit_raw[0])
                            # If it's a dict, get the @id
                            elif isinstance(unit_raw, dict):
                                unit_val = unit_raw.get("@id", "None")
                            # Otherwise, just stringify it
                            else:
                                unit_val = str(unit_raw)

                            summary_list.append({
                                "Label": item.get("skos:altLabel", "N/A"),
                                "Type": item.get("@type", "N/A"),
                                "Unit": unit_val,
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
