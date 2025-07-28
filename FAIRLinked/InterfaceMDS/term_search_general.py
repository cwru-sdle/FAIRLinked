import rdflib
from rdflib import Graph, RDFS, Namespace
import FAIRLinked.InterfaceMDS.load_mds_ontology
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph

def term_search_general(query_term=None, search_types=None):
    """
    Search an RDF ontology for subjects with a specified predicate and optional query term.

    Args:
        query_term (str or None): Optional term to match against the object of the predicate.
                                  If None, all values will be returned for the given search types.
        search_types (list[str]): List of search types: "Domain", "SubDomain", or "Study Stage".

    Prints:
        - A list of labels for matching subjects, grouped by search type.
    """
    # Define namespace
    MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")

    # Load ontology
    mds_ontology_graph = load_mds_ontology_graph()

    # Predicate map
    type_to_pred = {
        "Domain": MDS.hasDomain,
        "SubDomain": MDS.hasSubDomain,
        "Study Stage": MDS.hasStudyStage,
    }

    if search_types is None:
        print("No search types specified.")
        return

    if query_term is not None:
        query_term = query_term.lower()

    any_matches = False

    for search_type in search_types:
        if search_type not in type_to_pred:
            print(f"Unsupported search type: {search_type}")
            continue

        pred = type_to_pred[search_type]
        matches = set()

        for subj, obj in mds_ontology_graph.subject_objects(predicate=pred):
            if query_term is None or str(obj).lower() == query_term:
                matches.add(subj)

        if matches:
            any_matches = True
            print(f"\nTerms with {search_type}" + (f" matching '{query_term}'" if query_term else "") + ":")
            for s in sorted(matches, key=lambda x: str(x)):
                label = mds_ontology_graph.value(subject=s, predicate=RDFS.label)
                label_str = str(label) if label else f"[no label for {s}]"
                print(f"  {label_str}")

    if not any_matches:
        print("No matches found.")








    