import rdflib
from rdflib import Graph, SKOS, RDF, RDFS, OWL, DCAT, DCTERMS, Namespace, Literal, URIRef
import FAIRLinked.InterfaceMDS.load_mds_ontology
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import os


DOMAIN_SUBDOMAIN_MAPPING = {"Chem": ["Chem-Rxn"], 
    "Charact": ["Opt-UvVis", "Material-Profil", "Material-LPBF", "Material-AFM", "Opt-FTIR", "Material-Pyrom", "Material-Fract", "Material-XRD", "Elec-SIR", "Elec-Interdig"], 
    "BuiltEnv": ["PV-Cell", "PV-Module", "PV-Site", "PV-CrgCont"], 
    "Exposure": ["Accelerated"]}

MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")

def domain_subdomain_viewer():
    """
    Display all unique domain and subdomain values from an RDF ontology file.

    This function parses the RDF graph from the given file path and prints the unique
    values of the predicates `mds:hasDomain` and `mds:hasSubDomain`. Uniqueness is determined
    in a case-insensitive manner.

    Prints:
        - A list of unique domain values.
        - A list of unique subdomain values.
    """

    mds_ontology_graph = load_mds_ontology_graph()

    def normalize(obj):
        if isinstance(obj, (Literal, URIRef)):
            return str(obj).lower()
        return str(obj).lower()  # fallback

    # Use a dictionary to preserve original object for display, but deduplicate on lowercase
    unique_domains = {}
    unique_subdomains = {}

    for obj in mds_ontology_graph.objects(predicate=MDS.hasDomain):
        key = normalize(obj)
        if key not in unique_domains:
            unique_domains[key] = obj

    for obj in mds_ontology_graph.objects(predicate=MDS.hasSubDomain):
        key = normalize(obj)
        if key not in unique_subdomains:
            unique_subdomains[key] = obj

    print("Unique Domains (case-insensitive):")
    for obj in unique_domains.values():
        print(f"  {obj}")

    print("\nUnique SubDomains (case-insensitive):")
    for obj in unique_subdomains.values():
        print(f"  {obj}")

    
def domain_subdomain_directory(
    dsm=DOMAIN_SUBDOMAIN_MAPPING, 
    onto_graph: Graph = None, 
    output_dir: str = None
):
    """
    Displays an ASCII tree of domain-subdomain structure and optionally splits an RDF ontology 
    into separate Turtle files for each domain/subdomain combination.

    PARAMETERS
    ----------
    dsm : dict, optional
        A dictionary mapping domain names (str) to lists of subdomain names (list of str). 
        Default is `DOMAIN_SUBDOMAIN_MAPPING`.
        Example:
            {
                "Chem": ["Chem-Rxn"],
                "Charact": ["Opt-UvVis", "Material-Profil", ...],
                ...
            }

    onto_graph : rdflib.Graph, optional
        An RDF graph containing ontology data. Classes in this graph must have the predicates:
            - `mds:hasDomain` → string indicating domain
            - `mds:hasSubDomain` → string indicating subdomain
        If None, the function will only print the ASCII tree without creating Turtle files.

    output_dir : str, optional
        Path to the folder where domain/subdomain Turtle files will be saved. 
        Each domain gets a subfolder, and each subdomain gets a `.ttl` file. 
        If None, no files are created.

    BEHAVIOR
    --------
    1. Prints an ASCII tree of all domains and subdomains in `dsm`. 
       Example output:
           ├── Chem
           │   └── Chem-Rxn
           ├── Charact
           │   ├── Opt-UvVis
           │   ├── Material-Profil
           │   ...
           └── Exposure
               └── Accelerated

    2. If `onto_graph` and `output_dir` are provided:
        - Creates a folder structure `output_dir/domain/subdomain.ttl`.
        - For each `owl:Class` in `onto_graph` with `mds:hasDomain` and `mds:hasSubDomain`:
            - If its domain and subdomain match `dsm`, writes all triples about that class
              into the corresponding Turtle file.
        - Only writes files for classes that exist in the ontology.
        - Prints a confirmation for each file written, e.g.:
              ✅ Wrote split_ttl/Charact/Elec-SIR.ttl

    RETURNS
    -------
    None
        The function prints output to stdout and optionally writes Turtle files to disk.
    """

    # Draw the ASCII tree
    for i, (domain, subdomains) in enumerate(dsm.items()):
        is_last_domain = (i == len(dsm) - 1)
        domain_prefix = "└── " if is_last_domain else "├── "
        print(domain_prefix + domain)

        for j, sub in enumerate(subdomains):
            is_last_sub = (j == len(subdomains) - 1)
            sub_prefix = "    " if is_last_domain else "│   "
            branch = "└── " if is_last_sub else "├── "
            print(sub_prefix + branch + sub)

    # If no graph or output_dir given, stop here
    if onto_graph is None or output_dir is None:
        return

    # Make sure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # For each domain-subdomain, collect triples
    for domain, subdomains in dsm.items():
        domain_dir = os.path.join(output_dir, domain)
        os.makedirs(domain_dir, exist_ok=True)

        for sub in subdomains:
            g_sub = Graph()
            g_sub.bind("mds", MDS)
            g_sub.bind("owl", OWL)
            g_sub.bind("rdfs", RDFS)
            g_sub.bind("skos", SKOS)

            # Find all classes that match domain + subdomain
            for s in onto_graph.subjects(RDF.type, OWL.Class):
                has_domain = onto_graph.value(s, MDS.hasDomain)
                has_sub = onto_graph.value(s, MDS.hasSubDomain)

                if has_domain and has_sub:
                    if str(has_domain) == domain and str(has_sub) == sub:
                        # Add all triples about this subject
                        for p, o in onto_graph.predicate_objects(s):
                            g_sub.add((s, p, o))
                        # Also keep rdf:type triple
                        g_sub.add((s, RDF.type, OWL.Class))

            # Only save if we found something
            if len(g_sub) > 0:
                file_path = os.path.join(domain_dir, f"{sub}.ttl")
                g_sub.serialize(destination=file_path, format="turtle")
                print(f"✅ Wrote {file_path}")


def domain_subdomain_dir_interface():
    """
    Interactive CLI for creating a directory of ontology Turtle files based on
    domains and subdomains.

    - If the user chooses "yes":
        * Ask for the output directory path.
        * Ask whether to provide a custom ontology file path or use the default loader.
        * Load the ontology graph accordingly.
        * Call domain_subdomain_directory(onto_graph=..., output_dir=...).

    - If the user chooses "no":
        * Just print the ASCII tree using domain_subdomain_directory() with default args.
    """
    make_dir = input(
        "Would you like to make a directory of ontology files based on domains and subdomains (yes/no): "
    ).strip().lower()

    if make_dir == "yes":
        output_dir = input("Enter the output directory path: ").strip()
        os.makedirs(output_dir, exist_ok=True)

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
        # Just print the ASCII tree
        domain_subdomain_directory()