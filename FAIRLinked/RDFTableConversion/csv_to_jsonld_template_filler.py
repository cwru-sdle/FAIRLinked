import os
import json
import copy
import random
import string
import warnings
from datetime import datetime
import uuid
import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, SKOS, OWL, RDFS
from urllib.parse import quote

import os
import json
import copy
import random
import string
import warnings
from datetime import datetime
from urllib.parse import quote
import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace

def extract_data_from_csv(
    metadata_template,
    csv_file,
    row_key_cols,
    orcid,
    output_folder,
    prop_column_pair_dict=None,  # optional
    ontology_graph=None,          # optional
    base_uri="https://cwrusdle.bitbucket.io/mds/"
):
    """
    Converts CSV rows into RDF graphs using a JSON-LD template and optional property mapping,
    writing JSON-LD files.

    Parameters
    ----------
    metadata_template : dict
        JSON-LD template with "@context" and "@graph".

    csv_file : str
        Path to the input CSV.

    row_key_cols : list[str]
        Columns to uniquely identify each row.

    orcid : str
        ORCID identifier (dashes removed automatically).

    output_folder : str
        Directory to save JSON-LD files.

    prop_column_pair_dict : dict or None, optional
        Maps property keys to (subject_column, object_column) column pairs.
        If None or empty, no properties are added.

    ontology_graph : RDFLib Graph object or None, optional
        Ontology for property type/URI resolution.
        Required if prop_column_pair_dict is provided.

    base_uri : str, optional
        Base URI used to construct subject and object URIs.

    Returns
    -------
    List[rdflib.Graph]
        List of RDFLib Graphs, one per row.
    """

    df = pd.read_csv(csv_file)
    results = []
    orcid = orcid.replace("-", "")
    context = metadata_template.get("@context", {})
    graph_template = metadata_template.get("@graph", [])

    if prop_column_pair_dict:
        if ontology_graph is None:
            raise ValueError("ontology_graph must be provided if prop_column_pair_dict is used")
        prop_metadata_dict = generate_prop_metadata_dict(ontology_graph)
    else:
        prop_metadata_dict = {}

    for idx, row in df.iloc[2:].iterrows():
        try:
            # Generate row key and full identifier
            row_key_val = [str(row[col]).strip() for col in row_key_cols if col in row and pd.notna(row[col])]
            row_key = "-".join(row_key_val)
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            full_row_key = f"{row_key}-{orcid}-{timestamp}"

            # Deep copy the template and assign @id
            template_copy = copy.deepcopy(graph_template)
            subject_lookup = {}  # Maps skos:altLabel → generated @id

            for item in template_copy:
                if "@type" not in item or not item["@type"]:
                    warnings.warn(f"Missing or empty @type in template item: {item}")
                    continue
                if "skos:altLabel" not in item or not item["skos:altLabel"]:
                    raise ValueError("Missing skos:altLabel in template")

                prefix, localname = item["@type"].split(":")
                subject_uri = f"{context[prefix]}{localname}.{full_row_key}"
                item["@id"] = subject_uri
                subject_lookup[item["skos:altLabel"]] = URIRef(subject_uri)

                if "prov:generatedAtTime" in item:
                    item["prov:generatedAtTime"]["@value"] = datetime.utcnow().isoformat() + "Z"

                if "qudt:hasUnit" in item and not item["qudt:hasUnit"].get("@id"):
                    del item["qudt:hasUnit"]
                if "qudt:hasQuantityKind" in item and not item["qudt:hasQuantityKind"].get("@id"):
                    del item["qudt:hasQuantityKind"]

            jsonld_data = {
                "@context": context,
                "@graph": template_copy
            }

            g = Graph(identifier=URIRef(f"{base_uri}{full_row_key}"))
            g.parse(data=json.dumps(jsonld_data), format="json-ld")

            QUDT = Namespace("http://qudt.org/schema/qudt/")
            for alt_label, subj_uri in subject_lookup.items():
                if alt_label in row:
                    g.remove((subj_uri, QUDT.value, None))
                    g.add((subj_uri, QUDT.value, Literal(row[alt_label])))

            # Add object/datatype properties if given
            if prop_column_pair_dict:
                for key, column_pair_list in prop_column_pair_dict.items():
                    prop_metadata = prop_metadata_dict.get(key)
                    if not prop_metadata:
                        continue
                    prop_uri, prop_type = prop_metadata
                    pred_uri = URIRef(prop_uri)

                    for subj_col, obj_col in column_pair_list:
                        if subj_col not in row or pd.isna(row[subj_col]):
                            continue
                        alt_label = subj_col
                        subj_uri = subject_lookup.get(alt_label)
                        if not subj_uri:
                            continue

                        obj_val = row[obj_col]
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

            # Save the RDF graph to file
            random_suffix = ''.join(random.choices(string.ascii_lowercase, k=2))
            output_file = os.path.join(output_folder, f"{random_suffix}-{full_row_key}.jsonld")
            g.serialize(destination=output_file, format="json-ld", context=context, indent=2)
            results.append(g)

        except Exception as e:
            warnings.warn(f"Error processing row {idx} with key {row_key if 'row_key' in locals() else 'N/A'}: {e}")
            continue

    return results


def generate_prop_metadata_dict(ontology_graph):
    """
    Generates a dictionary where the keys are human-readable labels of object/datatype properties, and the values are
    2-tuples that contain the URI of that property in the first entry and the type (object/datatype) in second entry.

    Parameters
    ----------
    ontology_graph : RDFLib graph object of the ontology
        Path to the RDF/OWL ontology file.

    Returns
    -------
    dict
        Dictionary of the form:
        {
            "has material": ("http://example.org/ontology#hasMaterial", "Object Property"),
            "has value": ("http://example.org/ontology#hasValue", "Datatype Property"),
            ...
        }
    """

    prop_metadata_dict = {}

    for prop_type, label_type in [(OWL.ObjectProperty, "Object Property"), (OWL.DatatypeProperty, "Datatype Property")]:
        for prop in ontology_graph.subjects(RDF.type, prop_type):
            label = ontology_graph.value(prop, RDFS.label)
            if label:
                prop_metadata_dict[str(label)] = (str(prop), label_type)

    return prop_metadata_dict


def extract_from_folder(
    csv_folder, 
    metadata_template, 
    row_key_cols, orcid, 
    output_base_folder, 
    prop_column_pair_dict=None, 
    ontology_graph=None,
    base_uri="https://cwrusdle.bitbucket.io/mds/"
    ):
    """
    Processes all CSV files in a folder and converts each into RDF/JSON-LD files
    using a metadata template and optional object/datatype property mappings.

    Parameters
    ----------
    csv_folder : str
        Path to the folder containing CSV files.

    metadata_template : dict
        JSON-LD metadata template with "@context" and "@graph" describing the RDF structure.

    row_key_cols : list[str]
        List of CSV column names used to construct a unique key for each row.

    orcid : str
        ORCID iD of the user (dashes will be removed automatically).

    output_base_folder : str
        Directory where output subfolders (one per CSV) will be created for JSON-LD files.

    prop_column_pair_dict : dict or None, optional
        Mapping from property key (e.g., predicate label) to list of (subject_column, object_column) tuples.
        These define additional object or datatype properties to inject based on CSV columns.
        If None, no extra connections are added.

    ontology_graph : str or None, optional
        RDFLib graph object of ontology from which property URIs and types are resolved.
        Required only if `prop_column_pair_dict` is given.

    base_uri : str, optional
        Base URI used to construct RDF subject and object URIs. Defaults to the CWRU MDS base.

    Returns
    -------
    None
        Writes JSON-LD files to disk. No return value.
    """

    os.makedirs(output_base_folder, exist_ok=True)
    orcid = orcid.replace("-", "")

    for filename in os.listdir(csv_folder):
        if not filename.endswith(".csv"):
            continue

        csv_path = os.path.join(csv_folder, filename)

        types_used = [
            entry["@type"].split(":")[-1]
            for entry in metadata_template.get("@graph", [])
            if "@type" in entry and entry.get("skos:altLabel") in row_key_cols
        ]

        type_suffix = "-".join(set(types_used)) or "Unknown"
        uid = str(uuid.uuid4())[:8]
        folder_name = f"Dataset-{uid}-{type_suffix}"
        output_folder = os.path.join(output_base_folder, folder_name)

        os.makedirs(output_folder, exist_ok=True)
        extract_data_from_csv(metadata_template, csv_path, row_key_cols, orcid, output_folder, prop_column_pair_dict, ontology_graph, base_uri)
