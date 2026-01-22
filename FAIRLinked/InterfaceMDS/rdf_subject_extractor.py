import os
import pandas as pd
from rdflib import Graph, RDFS
from rdflib.namespace import DCTERMS, DC, SKOS
from fuzzysearch import find_near_matches
import FAIRLinked.InterfaceMDS.load_mds_ontology
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph


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




def get_adaptive_distance(keyword):
    """Determines allowed typos based on keyword length."""
    length = len(keyword)
    if length <= 2: return 0
    elif length <= 5: return 1
    else: return 2

def fuzzy_search_interface():
    """
    CLI interface compatible with extract_subject_details and fuzzy_filter_subjects_strict.
    """
    output_dir = input("Enter path to directory: ")
    if not os.path.exists(output_dir):
        print(f"❌ Directory not found: {output_dir}")
        return

    graph = load_mds_ontology_graph()
    df = extract_subject_details(graph)

    keywords_input = input("🔍 Enter keywords (comma-separated): ").strip()
    if not keywords_input:
        return

    keywords = [kw.strip() for kw in keywords_input.split(",") if kw.strip()]
    all_results = []

    print("\n--- Processing Search ---")
    for kw in keywords:
        dist = get_adaptive_distance(kw)
        match_df = fuzzy_filter_subjects_strict(df, [kw], column="label", max_l_dist=dist).copy()
        
        if not match_df.empty:
            match_df['searched_keyword'] = kw
            all_results.append(match_df)

    if all_results:
        final_df = pd.concat(all_results).drop_duplicates(subset=['subject_id'])

        print(f"\n✨ Found {len(final_df)} unique matches:")
        
        # --- FIXED SECTION: ONLY SHOW LABEL AND KEYWORD ---
        cols_to_show = ['searched_keyword', 'label'] 
        available_cols = [c for c in cols_to_show if c in final_df.columns]
        
        # Using a simple to_string() to avoid the red underline error
        print(final_df[available_cols].to_string(index=False))
        print("-" * 40)

        # Save to CSV (full data including subject_id and info will still be in the file)
        safe_kw_str = "_".join(keywords)[:50] 
        filename = f"fuzzy_search_{safe_kw_str}.csv"
        fuzzy_out = os.path.join(output_dir, filename)
        
        final_df.to_csv(fuzzy_out, index=False)
        print(f"✅ Full details (including URIs) saved to: {fuzzy_out}")
    else:
        print("No matches found.")



