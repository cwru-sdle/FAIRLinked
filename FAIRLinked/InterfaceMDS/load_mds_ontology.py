import rdflib
from rdflib import Graph, OWL
import requests

def load_mds_ontology_graph():
    """
    Attempts to load the MDS ontology RDF graph by following redirects and 
    using content negotiation to request different RDF serialization formats.

    Tries to fetch the ontology in the following formats (in order of preference):
        1. Turtle ("text/turtle")
        2. JSON-LD ("application/ld+json")
        3. RDF/XML ("application/rdf+xml")

    If a request is successful, the RDF data is parsed into an rdflib Graph.
    It also prints the `owl:versionInfo` if available, to indicate the version
    of the ontology that was loaded.

    Returns:
        rdflib.Graph:
            The parsed RDF graph of the MDS ontology, or `None` if all attempts fail.

    Raises:
        None explicitly. All exceptions are caught and printed.
    """
    mds_ontology_url = "https://w3id.org/mds/"
    timeout = 10

    headers_list = [
        ("text/turtle", "turtle"),
        ("application/ld+json", "json-ld"),
        ("application/rdf+xml", "xml")
    ]

    for accept_header, rdflib_format in headers_list:
        try:
            response = requests.get(
                mds_ontology_url,
                headers={"Accept": accept_header},
                allow_redirects=True,
                timeout=timeout
            )
            response.raise_for_status()

            mds_ontology_graph = Graph()
            mds_ontology_graph.parse(data=response.text, format=rdflib_format)

            # Get version info (use "Unknown" as default if not found)
            ontology_version = next(
                mds_ontology_graph.objects(subject=None, predicate=OWL.versionInfo),
                "Unknown"
            )
            print(f"Successfully loaded MDS-Onto version: {ontology_version} in {rdflib_format} format")
            return mds_ontology_graph

        except Exception as e:
            print(f"Attempt to retrieve {rdflib_format} version of MDS-Onto failed - {e}")

    print("All attempts at retrieving MDS-Onto failed.")
    return None



