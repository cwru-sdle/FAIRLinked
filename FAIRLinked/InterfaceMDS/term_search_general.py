import rdflib
from rdflib import Graph, RDFS, Namespace
import FAIRLinked.InterfaceMDS.load_mds_ontology
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph


def term_search_general(mds_ontology_graph=None, query_term=None, search_types=None, ttl_extr=False, ttl_path=None):
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

    if ttl_extr and ttl_path is None:
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

    if not search_types:
        print("No search types specified.")
        return

    if query_term:
        query_term = query_term.lower()

    # Step 1: Collect all unique subjects that match any of the criteria.
    all_matching_subjects = set()
    
    for search_type in search_types:
        pred = type_to_pred.get(search_type)
        if not pred:
            print(f"Unsupported search type: {search_type}")
            continue

        # Find subjects that match for the current search_type
        for subj, obj in mds_ontology_graph.subject_objects(predicate=pred):
            if query_term is None or str(obj).lower() == query_term:
                all_matching_subjects.add(subj)

    # Now, check if we found anything at all.
    if not all_matching_subjects:
        print("No matches found.")
        return

    # Print the human-readable results first
    print("\nFound matching subjects:")
    for s in sorted(all_matching_subjects, key=lambda x: str(x)):
        label = mds_ontology_graph.value(subject=s, predicate=RDFS.label)
        label_str = str(label) if label else f"[no label for {s}]"
        print(f"  {label_str}")

    # Step 2: If extraction is enabled, build and save the results graph.
    if ttl_extr:
        results_graph = Graph()
            
        # Copy all namespace prefixes from the original graph to the new one
        for prefix, namespace in mds_ontology_graph.namespace_manager.namespaces():
            results_graph.bind(prefix, namespace)
            
        # For each subject we found, get ALL its triples from the main graph
        for subj in all_matching_subjects:
            # This query (subj, None, None) fetches all triples for that subject.
            for triple in mds_ontology_graph.triples((subj, None, None)):
                results_graph.add(triple)
            
            # Finally, save the complete graph to the file ONCE, after the loops.
        print(f"\nSaving {len(results_graph)} triples to {ttl_path}...")
        results_graph.serialize(destination=ttl_path, format="turtle")
        print("Save complete.")

def filter_interface(args):

    """
    Term search using Domain, SubDomain, or Study Stage. For complete list of Domains and SubDomains, 
    run the following commands in bash:

    FAIRLinked view-domains
    FAIRLinked dir-make. 

    The current list of Study Stages include: 
    Synthesis, 
    Formulation, 
    Materials Processing, 
    Sample, 
    Tool, 
    Recipe, 
    Result,
    Analysis,
    Modelling.

    For more details about Study Stages, please view go see https://cwrusdle.bitbucket.io/.

    """
    
    if args.ontology_path == "default":
        ontology_graph = load_mds_ontology_graph()
    else:
        ontology_graph = Graph()
        ontology_graph.parse(args.ontology_path)

    if args.ttl_extr == "F":
        args.ttl_extr = False
    elif args.ttl_extr == "T":
        args.ttl_extr = True
    
    term_search_general(mds_ontology_graph=ontology_graph, 
                        query_term=args.query_term, 
                        search_types=args.search_types, 
                        ttl_extr=args.ttl_extr, 
                        ttl_path=args.ttl_path)











    