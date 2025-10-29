import pandas as pd
import json
import re
import os
import difflib
import rdflib
from datetime import datetime
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS, split_uri
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph

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
    MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
    terms = []
    for s in ontology_graph.subjects(RDF.type, OWL.Class):
        # Get both altLabels and rdfs:labels
        labels = list(ontology_graph.objects(s, SKOS.altLabel)) + list(ontology_graph.objects(s, RDFS.label))
        # Get definitions
        term_definitions = list(ontology_graph.objects(s, SKOS.definition))
        definition = str(term_definitions[0]) if term_definitions else ""
        study_stage = list(ontology_graph.objects(s, MDS.hasStudyStage))
        for label in labels:
            label_str = str(label).strip()
            terms.append({
                "iri": str(s),
                "label": label_str,
                "normalized": normalize(label_str),
                "definition": definition,
                "study_stage": study_stage
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


def jsonld_template_generator(csv_path, ontology_graph, output_path, matched_log_path, unmatched_log_path):
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
        if col == "__source_file__":
            continue
        
        match = find_best_match(col, ontology_terms)
        iri_fragment = str(match["iri"]).split("/")[-1].split("#")[-1] if match else normalize(col)
        iri = match["iri"] if match else f"mds:{iri_fragment}"
        definition = str(match["definition"]) if match else "Definition not available"
        study_stage = match["study_stage"] if match else ["Study stage information not available"]

        if match:
            matched_log.append(f"{col} => {iri_fragment}")

        else:
            unmatched_log.append(col)

        entry = {
            "@id": f"{iri}",
            "@type": f"{iri}",
            "skos:altLabel": col,
            "skos:definition": definition,
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
            "mds:hasStudyStage": study_stage
        }
        jsonld["@graph"].append(entry)

    # Ensure output directories exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.makedirs(os.path.dirname(matched_log), exist_ok=True)
    os.makedirs(os.path.dirname(unmatched_log), exist_ok=True)

    jsonld_string = json.dumps(jsonld, indent=2)
    # Write JSON-LD
    template_graph = Graph()
    template_graph.parse(data=jsonld_string, format="json-ld")
    template_graph.serialize(destination=output_path, format="json-ld", auto_compact=True)

    # Write matched log
    with open(matched_log_path, "w") as f:
        f.write("\n".join(matched_log))

    # Write unmatched log (remove duplicates with set)
    with open(unmatched_log_path, "w") as f:
        f.write("\n".join(sorted(set(unmatched_log))))  # BUG FIX: previously had stray '-' before 'fix'

def jsonld_temp_gen_interface(args):

    if args.ontology_path == "default":
        ontology_graph = load_mds_ontology_graph()
    else:
        ontology_graph = Graph()
        ontology_graph.parse(source=args.ontology_path)

    matched_path = os.path.join(args.log_path, "matched.txt")
    unmatched_path = os.path.join(args.log_path, "unmatched.txt")

    jsonld_template_generator(csv_path=args.csv_path, ontology_graph=ontology_graph, output_path=args.output_path, matched_log_path=matched_path, unmatched_log_path=unmatched_path)
    
