import pandas as pd
import json
from datetime import datetime
import os
import re
import difflib
import rdflib
from rdflib.namespace import RDF, RDFS, OWL, SKOS

def normalize(text):
    """
    Normalize a string by converting it to lowercase and removing non-alphanumeric characters.

    Args:
        text (str): The input string.

    Returns:
        str: The normalized string.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

def extract_terms_from_ontology(ontology_path):
    """
    Extract terms (classes and their labels) from an ontology file in Turtle (.ttl) format.

    Args:
        ontology_path (str): Path to the ontology TTL file.

    Returns:
        list of dict: A list of dictionaries with keys 'iri', 'label', and 'normalized'.
    """
    g = rdflib.Graph()
    g.parse(ontology_path, format="ttl")

    terms = []
    for s in g.subjects(RDF.type, OWL.Class):
        labels = list(g.objects(s, SKOS.altLabel)) + list(g.objects(s, RDFS.label))
        for label in labels:
            label_str = str(label).strip()
            terms.append({
                "iri": s,
                "label": label_str,
                "normalized": normalize(label_str)
            })
    return terms

def find_best_match(column, ontology_terms):
    """
    Find the best matching ontology term for a given CSV column name.

    Args:
        column (str): The column name from the CSV file.
        ontology_terms (list of dict): List of ontology terms with normalized labels.

    Returns:
        dict or None: Best match term dictionary or None if no close match is found.
    """
    norm_col = normalize(column)
    matches = [term for term in ontology_terms if term["normalized"] == norm_col]

    if matches:
        return matches[0]

    all_norm = [term["normalized"] for term in ontology_terms]
    close_matches = difflib.get_close_matches(norm_col, all_norm, n=1, cutoff=0.8)

    if close_matches:
        match_norm = close_matches[0]
        return next(term for term in ontology_terms if term["normalized"] == match_norm)

    return None

def convert_csv_to_jsonld(csv_path, ontology_path, output_path, matched_log_path, unmatched_log_path):
    """
    Convert a CSV file into a JSON-LD representation using an ontology for semantic enrichment.

    Args:
        csv_path (str): Path to the input CSV file.
        ontology_path (str): Path to the TTL ontology file.
        output_path (str): Path to output the resulting JSON-LD file.
        matched_log_path (str): Path to save the matched column log.
        unmatched_log_path (str): Path to save the unmatched column log.

    Returns:
        None
    """
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    ontology_terms = extract_terms_from_ontology(ontology_path)

    matched_log = []
    unmatched_log = []

    jsonld = {
        "@context": {
            "mds": "http://example.org/mds/",
            "schema": "http://schema.org/",
            "dcterms": "http://purl.org/dc/terms/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "qudt": "http://qudt.org/schema/qudt/",
            "prov": "http://www.w3.org/ns/prov#",
            "unit": "http://qudt.org/vocab/unit/",
            "quantitykind": "http://qudt.org/vocab/quantitykind/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "wd": "http://www.wikidata.org/entity/"
        },
        "@id": "mds:dataset",
        "dcterms:created": {
            "@value": datetime.today().strftime('%Y-%m-%d'),
            "@type": "xsd:dateTime"
        },
        "@graph": []
    }

    for col in columns:
        match = find_best_match(col, ontology_terms)
        if match:
            iri = str(match["iri"]).split("/")[-1].split("#")[-1]
            matched_log.append(f"{col} => {iri}")
        else:
            iri = col
            unmatched_log.append(col)

        entry = {
            "@id": f"mds:{iri}",
            "@type": f"mds:{iri}",
            "skos:altLabel": col,
            "skos:definition": "",
            "qudt:value": [{"@value": ""}],
            "qudt:hasUnit": {"@id": ""},
            "qudt:hasQuantityKind": {"@id": ""},
            "prov:generatedAtTime": {
                "@value": "",
                "@type": "xsd:dateTime"
            },
            "skos:note": {
                "@value": "placeholder note for user to fill",
                "@language": "en"
            }
        }

        jsonld["@graph"].append(entry)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(jsonld, f, indent=2)
    print(f"✅ JSON-LD written to: {output_path}")

    with open(matched_log_path, "w") as f:
        f.write("\n".join(matched_log))
    print(f"🟢 Matched columns logged to: {matched_log_path}")

    with open(unmatched_log_path, "w") as f:
        f.write("\n".join(set(unmatched_log)))
    print(f"⚠️ Unmatched columns logged to: {unmatched_log_path}")

# === RUN ===
if __name__ == "__main__":
    """
    Entry point for running the script.
    Converts the specified CSV file to JSON-LD using an ontology for semantic annotation.
    Logs matched and unmatched columns to respective log files.
    """
    convert_csv_to_jsonld(
        csv_path="/Users/lambaritu67/Desktop/fairlinked/data/pv_modules_info/t50-iv-sv-netsem-modules-info.csv",
        ontology_path="/Users/lambaritu67/Desktop/fairlinked/data/Test_Ontology_FAIRLinked/MDS-Onto-BuiltEnv-PV-Module-v0.3.0.0.ttl",
        output_path="/Users/lambaritu67/Desktop/fairlinked/output/t50-netsem-validated.jsonld",
        matched_log_path="/Users/lambaritu67/Desktop/fairlinked/output/matched_columns.log",
        unmatched_log_path="/Users/lambaritu67/Desktop/fairlinked/output/unmatched_columns.log"
    )
