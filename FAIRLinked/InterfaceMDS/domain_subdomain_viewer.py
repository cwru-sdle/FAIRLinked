import rdflib
from rdflib import Graph, SKOS, RDF, RDFS, OWL, DCAT, DCTERMS, Namespace, Literal, URIRef
import FAIRLinked.InterfaceMDS.load_mds_ontology
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph


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

    MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
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