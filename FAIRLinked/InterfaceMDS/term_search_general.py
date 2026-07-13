from rdflib import Graph, RDFS, Namespace
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from .domain_subdomain_viewer import build_dynamic_dsm


def term_search_general(mds_ontology_graph=None, query_term=None, search_types=None, ttl_extr=False, ttl_path=None):
    """
    Search an RDF ontology for subjects with a specified predicate and optional query term.

    Args:
        mds_ontology_graph (rdflib.Graph, optional): An existing RDF graph. If None, one will be loaded.
        query_term (str, optional): Term to match against the object of the predicate.
                                    If None, all values will be returned for the given search types.
        search_types (list[str]): List of search types: "Domain", "SubDomain", or "Study Stage".
        ttl_extr (bool, optional): If True, extract the search results into a new graph. Defaults to False.
        ttl_path (str, optional): The file path to save the extracted turtle (.ttl) file.
                                  Required if ttl_extr is True.

    Prints:
        - A list of labels for matching subjects.
    """
    if ttl_extr and ttl_path is None:
        raise ValueError("A file path must be provided via ttl_path to save the results when ttl_extr is enabled.")

    MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")

    # Load ontology if not passed
    if mds_ontology_graph is None:
        mds_ontology_graph = load_mds_ontology_graph()

    if not search_types:
        print("No search types specified.")
        return

    # 1. Generate the dynamic map using URIRefs
    dsm = build_dynamic_dsm(mds_ontology_graph)

    # Clean the query term for case-insensitive matching
    query_clean = query_term.strip().lower() if query_term else None

    # Helper function to see if a URIRef string/label matches what the user typed
    def uri_matches_query(uri):
        if query_clean is None:
            return True
        
        label = mds_ontology_graph.value(subject=uri, predicate=RDFS.label)
        local_name = str(uri).split('#')[-1].split('/')[-1].lower()
        
        if label and str(label).lower() == query_clean:
            return True
        return local_name == query_clean

    # Set to collect matching subjects uniquely
    all_matching_subjects = set()
    
    for search_type in search_types:
        # --- Handle Study Stage (Legacy Attribute Match) ---
        if search_type == "Study Stage":
            for subj, obj in mds_ontology_graph.subject_objects(predicate=MDS.hasStudyStage):
                if query_clean is None or str(obj).lower() == query_clean:
                    all_matching_subjects.add(subj)

        # --- Handle SubDomain ---
        elif search_type == "SubDomain":
            # Collect all subdomains across the DSM values that match the string query
            matching_subdomains = set()
            for sub_list in dsm.values():
                for sub_uri in sub_list:
                    if uri_matches_query(sub_uri):
                        matching_subdomains.add(sub_uri)

            # Find subjects mapped to those validated subdomain URIs
            for sub_uri in matching_subdomains:
                for subj in mds_ontology_graph.subjects(predicate=MDS.inDomain, object=sub_uri):
                    all_matching_subjects.add(subj)

        # --- Handle Domain ---
        elif search_type == "Domain":
            # Identify which top-level domain keys match the query text
            matching_domains = [dom_uri for dom_uri in dsm.keys() if uri_matches_query(dom_uri)]

            for dom_uri in matching_domains:
                # Merge the top-level domain URI and its subdomains into valid targets
                allowed_targets = dsm[dom_uri] + [dom_uri]
                
                # Pull all classes flagged with any of these domain/subdomain targets
                for target_uri in allowed_targets:
                    for subj in mds_ontology_graph.subjects(predicate=MDS.inDomain, object=target_uri):
                        all_matching_subjects.add(subj)
        else:
            print(f"Unsupported search type: {search_type}")

    # Check if we found anything at all
    if not all_matching_subjects:
        print("No matches found.")
        return

    # Print the human-readable results
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
            for triple in mds_ontology_graph.triples((subj, None, None)):
                results_graph.add(triple)
            
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
    Modeling.

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











    