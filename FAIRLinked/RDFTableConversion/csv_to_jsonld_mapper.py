import pandas as pd
import json
import re
import os
import difflib
import rdflib
from datetime import datetime
from rdflib.namespace import RDF, RDFS, OWL, SKOS

def normalize(text):
    """
    Normalize a text string by converting it to lowercase and removing non-alphanumeric characters.

    Args:
        text (str): Input text to normalize.

    Returns:
        str: Normalized string.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())


def extract_terms_from_ontology(ontology_graph):
    """
    Extract terms from an RDF graph representing an OWL ontology.

    Args:
        ontology_graph (rdflib.Graph): The ontology RDF graph.

    Returns:
        list[dict]: A list of dictionaries containing term IRIs, original labels, and normalized labels.
    """
    terms = []
    for s in ontology_graph.subjects(RDF.type, OWL.Class):
        # Get both altLabels and rdfs:labels
        labels = list(ontology_graph.objects(s, SKOS.altLabel)) + list(ontology_graph.objects(s, RDFS.label))
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
    Find the best matching ontology term for a given column name.

    Args:
        column (str): The name of the column from the CSV file.
        ontology_terms (list[dict]): List of extracted ontology terms.

    Returns:
        dict or None: The best-matching ontology term, or None if no good match is found.
    """
    norm_col = normalize(column)

    # First, try exact normalized match
    matches = [term for term in ontology_terms if term["normalized"] == norm_col]
    if matches:
        return matches[0]

    # Otherwise, find close match using difflib
    all_norm = [term["normalized"] for term in ontology_terms]
    close_matches = difflib.get_close_matches(norm_col, all_norm, n=1, cutoff=0.8)

    if close_matches:
        match_norm = close_matches[0]
        return next(term for term in ontology_terms if term["normalized"] == match_norm)

    return None


def json_ld_template_generator(csv_path, ontology_graph, output_path, matched_log_path, unmatched_log_path):
    """
    Use a CSV file into a JSON-LD template that user can fill out column metadata.

    Args:
        csv_path (str): Path to the CSV file to generate JSON-LD template.
        ontology_graph (rdflib.Graph): The ontology RDF graph for matching terms.
        output_path (str): Path to write the resulting JSON-LD file.
        matched_log_path (str): Path to write the log of columns that matched the ontology.
        unmatched_log_path (str): Path to write the log of columns that can't be found in the ontology.
    """
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    ontology_terms = extract_terms_from_ontology(ontology_graph)

    matched_log = []
    unmatched_log = []

    # Construct the base JSON-LD structure
    jsonld = {
        "@context": {
            "mds": "https://cwrusdle.bitbucket.io/mds/",
            "schema": "http://schema.org/",
            "dcterms": "http://purl.org/dc/terms/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "qudt": "http://qudt.org/schema/qudt/",
            "prov": "http://www.w3.org/ns/prov#",
            "unit": "http://qudt.org/vocab/unit/",
            "quantitykind": "http://qudt.org/vocab/quantitykind/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "wd": "http://www.wikidata.org/entity/",
            "cco": "https://www.commoncoreontologies.org/"
        },
        "@id": "mds:dataset",
        "dcterms:created": {
            "@value": datetime.today().strftime('%Y-%m-%d'),
            "@type": "xsd:dateTime"
        },
        "@graph": []
    }

    # Process each column and attempt to match it to ontology terms
    for col in columns:
        match = find_best_match(col, ontology_terms)
        iri_fragment = str(match["iri"]).split("/")[-1].split("#")[-1] if match else col

        if match:
            matched_log.append(f"{col} => {iri_fragment}")
        else:
            unmatched_log.append(col)

        entry = {
            "@id": f"mds:{iri_fragment}",
            "@type": f"mds:{iri_fragment}",
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
            },
            "mds:hasStudyStage": {
                
            }
        }
        jsonld["@graph"].append(entry)

    # Ensure output directories exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write JSON-LD
    with open(output_path, "w") as f:
        json.dump(jsonld, f, indent=2)

    # Write matched log
    with open(matched_log_path, "w") as f:
        f.write("\n".join(matched_log))

    # Write unmatched log (remove duplicates with set)
    with open(unmatched_log_path, "w") as f:
        f.write("\n".join(sorted(set(unmatched_log))))  # BUG FIX: previously had stray '-' before 'fix'
