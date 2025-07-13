import os
import pandas as pd
from rdflib import Graph, RDFS
from rdflib.namespace import DCTERMS, DC, SKOS
from fuzzysearch import find_near_matches


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
        pd.DataFrame: DataFrame with 'subject_id', 'label', and 'info'.
    """
    results = []
    seen_subjects = set()

    for subj in set(graph.subjects()):
        if subj in seen_subjects:
            continue
        seen_subjects.add(subj)

        # Get preferred label
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

        # Collect predicate-object info
        po_pairs = [
            f"{pred.n3(graph.namespace_manager)} → {obj.n3(graph.namespace_manager)}"
            for pred, obj in graph.predicate_objects(subject=subj)
        ]

        results.append({
            "subject_id": str(subj),
            "label": label if label else "",
            "info": " | ".join(po_pairs)
        })

    return pd.DataFrame(results)


def fuzzy_filter_subjects_strict(df, keywords, column="label", max_l_dist=1):
    """
    Perform strict fuzzy word-level matching using Levenshtein distance.

    Args:
        df (pd.DataFrame): DataFrame to filter.
        keywords (list of str): Keywords to search for.
        column (str): Column to search.
        max_l_dist (int): Max Levenshtein distance.

    Returns:
        pd.DataFrame: Filtered DataFrame of matches.
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


# NOTE: This main() function is only for interactive CLI use
def main():
    """
    Example usage for local testing. Not included in the package version.
    """
    file_path = input("Enter path to RDF (.ttl) file: ").strip()
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return

    graph = load_graph_from_file(file_path)
    df = extract_subject_details(graph)

    output_csv = file_path.replace(".ttl", "_subjects.csv")
    df.to_csv(output_csv, index=False)
    print(f"\n📁 Full output saved to: {output_csv}")

    keywords_input = input("🔍 Enter fuzzy keywords (comma-separated, e.g., temp,pressure): ").strip()
    if keywords_input:
        keywords = [kw.strip() for kw in keywords_input.split(",")]
        max_dist = input("Enter max Levenshtein distance (default = 1): ").strip()
        max_dist = int(max_dist) if max_dist.isdigit() else 1

        filtered_df = fuzzy_filter_subjects_strict(df, keywords, max_l_dist=max_dist)
        fuzzy_out = output_csv.replace(".csv", f"_fuzzy_{'_'.join(keywords)}.csv")
        filtered_df.to_csv(fuzzy_out, index=False)
        print(f"✅ Fuzzy match output saved to: {fuzzy_out}")


if __name__ == "__main__":
    main()
