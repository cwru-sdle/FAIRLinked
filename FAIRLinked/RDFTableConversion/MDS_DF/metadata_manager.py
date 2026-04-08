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


        def add_column_metadata(self, 
                                col_name: str, 
                                rdf_type: str, 
                                unit: str = "UNITLESS", 
                                definition: str = "No definition provided", 
                                study_stage: str = "UNK"):
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

        def delete_column_metadata(self, col_name: str):
            """
            Removes all metadata associated with a specific column from the graph and template.

            Args:
                col_name (str): The column name (skos:altLabel) to be removed.
            """
            # 1. Find the subject subject (the node) that has the altLabel matching col_name
            subject = self.template_graph.value(predicate=SKOS.altLabel, object=Literal(col_name))

            if subject:
                # 2. Remove all triples where this node is the subject
                self.template_graph.remove((subject, None, None))
                
                # 3. Re-sync the dictionary to reflect the deletion
                context = self.metadata_temp.get("@context", {})
                updated_json = self.template_graph.serialize(format="json-ld", context=context)
                self.metadata_temp = json.loads(updated_json)
                
                print(f"✅ Successfully deleted metadata for column: '{col_name}'.")
            else:
                print(f"⚠️ Column '{col_name}' not found in the metadata.")
        
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