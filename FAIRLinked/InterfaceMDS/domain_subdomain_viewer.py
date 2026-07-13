import rdflib
from rdflib import Graph, SKOS, RDF, RDFS, OWL, DCAT, DCTERMS, Namespace, Literal, URIRef
import FAIRLinked.InterfaceMDS.load_mds_ontology
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import os


MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
MDSDOM = Namespace("https://cwrusdle.bitbucket.io/mdsdom/")

def domain_subdomain_viewer():
    """
    Display unique domain and subdomain values from an RDF ontology file.
    
    - Domains: Absolute top-level classes (no rdfs:subClassOf).
    - Subdomains: Any class in the hierarchy that IS a subclass of something else.
    """
    mds_ontology_graph = load_mds_ontology_graph()

    unique_domains = {}
    unique_subdomains = {}

    def get_label_or_uri(uri):
        label = mds_ontology_graph.value(subject=uri, predicate=RDFS.label)
        return str(label) if label else str(uri)

    def find_root_domain(uri):
        """Recursively climbs the rdfs:subClassOf tree to find the top-most domain."""
        parent = mds_ontology_graph.value(subject=uri, predicate=RDFS.subClassOf)
        # Stop climbing if there is no parent, or if it points to a generic owl:Class
        if not parent or parent == OWL.Class:
            return uri
        return find_root_domain(parent)

    # 1. Inspect every object targeted by MDS.inDomain
    for obj in mds_ontology_graph.objects(predicate=MDS.inDomain):
        
        # Check if it has a parent
        parent = mds_ontology_graph.value(subject=obj, predicate=RDFS.subClassOf)
        
        if parent and parent != OWL.Class:
            # It has a parent -> Subdomain
            if obj not in unique_subdomains:
                unique_subdomains[obj] = get_label_or_uri(obj)
            
            # Trace all the way to the top to get the absolute Root Domain
            root_domain = find_root_domain(obj)
            if root_domain not in unique_domains:
                unique_domains[root_domain] = get_label_or_uri(root_domain)
        else:
            # It has no parent -> It is an absolute top-level Domain
            if obj not in unique_domains:
                unique_domains[obj] = get_label_or_uri(obj)

    # 2. Print results
    print("Unique Domains:")
    for label in sorted(unique_domains.values()):
        print(f"  {label}")

    print("\nUnique SubDomains:")
    for label in sorted(unique_subdomains.values()):
        print(f"  {label}")

    

def build_dynamic_dsm(onto_graph):
    """
    Dynamically builds a domain-subdomain mapping from the ontology graph.
    
    Returns:
        dict: { domain_URIRef: [subdomain_URIRef, ...], ... }
    """
    dsm = {}
    
    def find_root_domain(uri):
        parent = onto_graph.value(subject=uri, predicate=RDFS.subClassOf)
        if not parent or parent == OWL.Class:
            return uri
        return find_root_domain(parent)

    # Scan everything bound via MDS.inDomain to discover relevant domains/subdomains
    for obj in onto_graph.objects(predicate=MDS.inDomain):
        parent = onto_graph.value(subject=obj, predicate=RDFS.subClassOf)
        
        if parent and parent != OWL.Class:
            # It's a subdomain! Find its absolute top-level domain root
            root_domain = find_root_domain(obj)
            if root_domain not in dsm:
                dsm[root_domain] = set()
            dsm[root_domain].add(obj)
        else:
            # It's a top-level domain with no subdomains yet
            if obj not in dsm:
                dsm[obj] = set()

    # Convert sets to sorted lists for predictable file generation & printing
    return {domain: sorted(list(subdomains)) for domain, subdomains in sorted(dsm.items())}


def domain_subdomain_directory(onto_graph: Graph = None, output_dir: str = None):
    """
    Dynamically parses the domain-subdomain tree from the ontology.
    Prints an ASCII tree and splits triples into matching directory structures.
    """
    # If no graph is passed, we fall back to loading the default one to draw the tree
    if onto_graph is None:
        onto_graph = load_mds_ontology_graph()

    # Generate the map dynamically from the graph metadata!
    dsm = build_dynamic_dsm(onto_graph)

    def get_label(uri):
        """Helper to print rdfs:label or fallback to fragment/string."""
        label = onto_graph.value(subject=uri, predicate=RDFS.label)
        if label:
            return str(label)
        # Fallback to local name/fragment if label doesn't exist
        return str(uri).split('#')[-1].split('/')[-1]

    # 1. Draw the ASCII tree using human-readable labels
    print("Ontology Domain Hierarchy:")
    domain_list = list(dsm.items())
    for i, (domain_uri, subdomain_uris) in enumerate(domain_list):
        is_last_domain = (i == len(domain_list) - 1)
        domain_prefix = "└── " if is_last_domain else "├── "
        print(f"{domain_prefix}{get_label(domain_uri)}")

        for j, sub_uri in enumerate(subdomain_uris):
            is_last_sub = (j == len(subdomain_uris) - 1)
            sub_prefix = "    " if is_last_domain else "│   "
            branch = "└── " if is_last_sub else "├── "
            print(f"{sub_prefix}{branch}{get_label(sub_uri)}")

    # Stop here if no folder splitting is requested
    if output_dir is None:
        return

    os.makedirs(output_dir, exist_ok=True)

    # 2. Split the ontology into files based on actual class allocations
    for domain_uri, subdomain_uris in dsm.items():
        domain_name = str(domain_uri).split('#')[-1].split('/')[-1] # Clean string for folder name
        domain_dir = os.path.join(output_dir, domain_name)
        os.makedirs(domain_dir, exist_ok=True)

        for sub_uri in subdomain_uris:
            sub_name = str(sub_uri).split('#')[-1].split('/')[-1] # Clean string for filename
            
            g_sub = Graph()
            # Inherit namespaces from main graph to keep serialized outputs pretty
            for prefix, ns in onto_graph.namespaces():
                g_sub.bind(prefix, ns)

            # Find classes explicitly stamped with this specific subdomain
            for s in onto_graph.subjects(predicate=MDS.inDomain, object=sub_uri):
                # Grab all triples descriptive of this entity class
                for p, o in onto_graph.predicate_objects(s):
                    g_sub.add((s, p, o))

            # Only serialize files for slices containing active data
            if len(g_sub) > 0:
                file_path = os.path.join(domain_dir, f"{sub_name}.ttl")
                g_sub.serialize(destination=file_path, format="turtle")
                print(f"✅ Wrote {file_path}")


def domain_subdomain_dir_interface():
    """Interactive CLI for creating a directory of ontology Turtle files."""
    make_dir = input(
        "Would you like to make a directory of ontology files based on domains and subdomains (yes/no): "
    ).strip().lower()

    if make_dir == "yes":
        output_dir = input("Enter the output directory path: ").strip()

        custom_onto = input(
            "Would you like to provide a path to an ontology file? (yes/no): "
        ).strip().lower()

        if custom_onto == "yes":
            onto_path = input("Enter the path to your ontology file: ").strip()
            if not os.path.isfile(onto_path):
                print(f"❌ Error: File not found at {onto_path}")
                return
            onto_graph = Graph()
            onto_graph.parse(onto_path, format="turtle")
        else:
            onto_graph = load_mds_ontology_graph()

        domain_subdomain_directory(onto_graph=onto_graph, output_dir=output_dir)
    else:
        # Just prints the dynamic ASCII tree
        domain_subdomain_directory()