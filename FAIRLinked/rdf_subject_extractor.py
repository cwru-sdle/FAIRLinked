import os
import pandas as pd
from rdflib import Graph, RDFS
from rdflib.namespace import DCTERMS, DC, SKOS
from fuzzysearch import find_near_matches

# ====== CONFIGURABLE PATH ======
RDF_FILE_PATH = "/Users/lambaritu67/Desktop/fairlinked/data/Test_Ontology_FAIRLinked/MDS_Onto-v0.3.0.0 copy.ttl"
OUTPUT_CSV_PATH = "expanded_subject_info.csv"


def load_graph_from_file(file_path):
    """
    Load an RDF file into an rdflib Graph object.

    Args:
        file_path (str): Path to the RDF file (.ttl or other RDF-supported formats).

    Returns:
        rdflib.Graph: Parsed RDF graph.
    """
    graph = Graph()
    try:
        graph.parse(file_path, format="turtle" if file_path.endswith(".ttl") else None)
        print(f"✅ Loaded RDF file: {file_path}")
    except Exception as e:
        print(f"❌ Failed to parse {file_path}: {e}")
    return graph


def extract_subject_details(graph):
    """
    Extract all subjects from an RDF graph, including preferred labels and predicate-object details.

    Args:
        graph (rdflib.Graph): The parsed RDF graph.

    Returns:
        pd.DataFrame: DataFrame with columns:
            - 'subject_id': URI of the subject
            - 'label': Preferred label (if any)
            - 'info': Concatenated string of predicate-object descriptions
    """
    results = []
    seen_subjects = set()

    for subj in set(graph.subjects()):
        if subj in seen_subjects:
            continue
        seen_subjects.add(subj)

        # Extract label from prioritized predicates
        label = None
        label_predicates = [
            SKOS.prefLabel,
            SKOS.altLabel,
            SKOS.hiddenLabel,
            RDFS.label,
            RDFS.comment,
            DCTERMS.subject,
            DC.subject,
        ]
        for predicate in label_predicates:
            label_obj = graph.value(subj, predicate)
            if label_obj:
                label = str(label_obj)
                break

        # Collect predicate-object pairs
        po_pairs = []
        for pred, obj in graph.predicate_objects(subject=subj):
            po_pairs.append(f"{pred.n3(graph.namespace_manager)} → {obj.n3(graph.namespace_manager)}")

        results.append({
            "subject_id": str(subj),
            "label": label if label else "",
            "info": " | ".join(po_pairs)
        })

    return pd.DataFrame(results)


def fuzzy_filter_subjects_strict(df, keywords, column="label", max_l_dist=1):
    """
    Perform strict word-level fuzzy matching against a column in the DataFrame using Levenshtein distance.

    Args:
        df (pd.DataFrame): DataFrame to search through.
        keywords (list of str): List of target keywords to match.
        column (str): Column to search (default is 'label').
        max_l_dist (int): Maximum Levenshtein distance for fuzzy matches.

    Returns:
        pd.DataFrame: Filtered DataFrame containing only matching rows.
    """
    matches = []
    keywords = [kw.lower() for kw in keywords]

    for _, row in df.iterrows():
        label = str(row[column]).lower()
        words = set(label.replace("-", " ").replace("_", " ").split())

        for word in words:
            for keyword in keywords:
                if find_near_matches(keyword, word, max_l_dist=max_l_dist):
                    matches.append(row)
                    break
            else:
                continue
            break

    return pd.DataFrame(matches)


if __name__ == "__main__":
    """
    Main execution block:
    - Loads RDF file
    - Extracts subject metadata and exports it
    - Optionally performs strict fuzzy filtering based on user input
    """
    graph = load_graph_from_file(RDF_FILE_PATH)
    df = extract_subject_details(graph)

    # Save full subject metadata
    df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"\n📁 Full output saved to: {OUTPUT_CSV_PATH}")

    # Prompt user for fuzzy keyword filtering
    keywords_input = input("🔍 Enter fuzzy keywords (comma-separated, e.g., temp,temperature): ").strip()
    if keywords_input:
        keywords = [kw.strip() for kw in keywords_input.split(",")]
        max_dist = input("Enter max Levenshtein distance (default 1): ").strip()
        max_dist = int(max_dist) if max_dist.isdigit() else 1

        filtered_df = fuzzy_filter_subjects_strict(df, keywords, max_l_dist=max_dist)
        output_path = OUTPUT_CSV_PATH.replace(".csv", f"_fuzzy_strict_{'_'.join(keywords)}.csv")
        filtered_df.to_csv(output_path, index=False)
        print(f"✅ Fuzzy strict match output saved to: {output_path}")
