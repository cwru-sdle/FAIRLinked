import rdflib
from rdflib import Graph, RDFS, Namespace
import FAIRLinked.InterfaceMDS.load_mds_ontology
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph

def term_search_general(mds_ontology_graph=None, query_term=None, search_types=None, ttl_extr=0, ttl_path=None):
    """
    Search an RDF ontology for subjects with a specified predicate and optional query term.

    Args:
        mds_ontology_graph (rdflib.Graph, optional): An existing RDF graph. If None, one will be loaded.
        query_term (str, optional): Term to match against the object of the predicate.
                                    If None, all values will be returned for the given search types.
        search_types (list[str]): List of search types: "Domain", "SubDomain", or "Study Stage".
        ttl_extr (int, optional): If not 0, extract the search results into a new graph. Defaults to 0.
        ttl_path (str, optional): The file path to save the extracted turtle (.ttl) file.
                                  Required if ttl_extr is not 0.


    Prints:
        - A list of labels for matching subjects, grouped by search type.
    """

    if ttl_extr != 0 and ttl_path is None:
        raise ValueError("A file path must be provided via ttl_path to save the results when ttl_extr is enabled.")

    # Define namespace
    MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")

    # Load ontology
    
    if mds_ontology_graph is None:
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

    results_graph = Graph() if ttl_extr != 0 else None

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
                # Add the found triple to our single results graph
                if results_graph is not None:
                    results_graph.add((subj, None, None))
            if ttl_extr == 1:
                results_graph.serialize(destination=ttl_path, format="turtle")
            

    if not any_matches:
        print("No matches found.")











    