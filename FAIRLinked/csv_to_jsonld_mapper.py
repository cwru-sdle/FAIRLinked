import pandas as pd
import json
import re
import os
import difflib
import rdflib
from datetime import datetime
from rdflib.namespace import RDF, RDFS, OWL, SKOS

def normalize(text):
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

def extract_terms_from_ontology(ontology_path):
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
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    ontology_terms = extract_terms_from_ontology(ontology_path)

    matched_log = []
    unmatched_log = []

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
        iri = str(match["iri"]).split("/")[-1].split("#")[-1] if match else col
        if match:
            matched_log.append(f"{col} => {iri}")
        else:
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

    with open(matched_log_path, "w") as f:
        f.write("\n".join(matched_log))

    with open(unmatched_log_path, "w") as f:
        f.write("\n".join(set(unmatched_log)))
