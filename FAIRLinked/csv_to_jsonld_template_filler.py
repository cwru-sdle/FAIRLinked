"""
csv_to_jsonld_template_filler.py

This module provides utilities to convert a folder of CSV files into JSON-LD files
using a metadata template conforming to FAIR and QUDT ontologies. It is designed for
integration into the FAIRLinked package for publishing FAIR RDF datasets.

Author: Ritika
SDLE Research Center, Case Western Reserve University
"""

import os
import json
import copy
import uuid
import random
import string
import warnings
from datetime import datetime

import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef, RDF, SKOS


def extract_data_from_csv(metadata_template, csv_file, row_key_cols, orcid, output_folder):
    """
    Converts each row of a CSV file into a JSON-LD file based on a provided metadata template.

    Args:
        metadata_template (dict): JSON-LD with @context and @graph.
        csv_file (str): Path to the input CSV file.
        row_key_cols (list[str]): Columns to build a unique row key.
        orcid (str): ORCID identifier (dashes will be removed).
        output_folder (str): Directory to save JSON-LD files.

    Returns:
        List[rdflib.Graph]: RDFLib graphs of the generated JSON-LD data.
    """
    df = pd.read_csv(csv_file)
    results = []
    orcid = orcid.replace("-", "")

    context = metadata_template.get("@context", {})
    graph_template = metadata_template.get("@graph", [])

    for _, row in df.iterrows():
        template_copy = copy.deepcopy(graph_template)
        row_key_val = [str(row[col]).strip() for col in row_key_cols if pd.notna(row[col]) and col in row]
        row_key = "-".join(row_key_val)
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        full_row_key = f"{row_key}-{orcid}-{timestamp}"

        for item in template_copy:
            if "@type" not in item or not item["@type"]:
                warnings.warn(f"Missing or empty @type in template item: {item}")
                continue
            if "skos:altLabel" not in item or not item["skos:altLabel"]:
                raise ValueError("Missing skos:altLabel in template")

            prefix, localname = item["@type"].split(":")
            item["@id"] = f"{prefix}:{localname}.{full_row_key}"

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

        g = Graph(identifier=URIRef(f"https://cwrusdle.bitbucket.io/mds#{full_row_key}"))
        g.parse(data=json.dumps(jsonld_data), format="json-ld")

        QUDT = Namespace("http://qudt.org/schema/qudt/")

        for item in template_copy:
            type_curie = item.get("@type")
            if not type_curie:
                continue

            prefix, localname = type_curie.split(":")
            type_uri = URIRef(f"{context[prefix]}{localname}")

            for subj in g.subjects(RDF.type, type_uri):
                col_name = str(g.value(subj, SKOS.altLabel))
                if col_name not in row:
                    continue
                g.remove((subj, QUDT.value, None))
                g.add((subj, QUDT.value, Literal(row[col_name])))

        random_suffix = ''.join(random.choices(string.ascii_lowercase, k=2))
        output_file = os.path.join(output_folder, f"{random_suffix}-{full_row_key}.jsonld")
        g.serialize(destination=output_file, format="json-ld", context=context, indent=2)
        results.append(g)

    return results


def extract_from_folder(csv_folder, metadata_template, row_key_cols, orcid, output_base_folder):
    """
    Processes all CSV files in a folder and converts each into JSON-LD using a metadata template.

    Args:
        csv_folder (str): Folder containing CSVs.
        metadata_template (dict): JSON-LD template for RDF structure.
        row_key_cols (list[str]): Columns used to create row keys.
        orcid (str): ORCID identifier (dashes will be stripped).
        output_base_folder (str): Where to save output subfolders of JSON-LD files.
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
        folder_name = f"DS{uid}-{type_suffix}"
        output_folder = os.path.join(output_base_folder, folder_name)

        os.makedirs(output_folder, exist_ok=True)
        extract_data_from_csv(metadata_template, csv_path, row_key_cols, orcid, output_folder)
