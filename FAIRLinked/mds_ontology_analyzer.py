from rdflib import Graph, Namespace, URIRef, RDF, OWL
from typing import Dict, Set, List, Tuple, Union
from collections import defaultdict
from FAIRLinked.utility import NAMESPACE_MAP, LIGHT_COLORS, CATEGORY_COLORS, ONTO_CORE_CATEGORIES

###############################################################################
# Global Namespaces
###############################################################################
MDS = Namespace(NAMESPACE_MAP['mds'])
RDFS = Namespace(NAMESPACE_MAP['rdfs'])
SKOS = Namespace(NAMESPACE_MAP['skos'])


###############################################################################
# Helper Functions
###############################################################################

def update_category_colors(categories: Set[str]) -> None:
    """
    Updates the global CATEGORY_COLORS dictionary with color assignments for each category.
    
    The function ensures that each category (in prefixed form e.g. 'mds:tool') is assigned 
    a unique color from the LIGHT_COLORS palette, cycling through colors if needed.
    
    High-level logic:
    1. Clear existing color assignments
    2. Convert full URIs to prefixed form (e.g. 'http://...#tool' -> 'mds:tool') 
    3. Assign colors from palette to each prefixed category
    4. Update ONTO_CORE_CATEGORIES with final set of categories
    
    Args:
        categories (Set[str]): Set of category URIs to assign colors to
                              (e.g. {'http://...#tool', 'http://...#recipe'})
    
    Global Effects:
        - Updates CATEGORY_COLORS with mappings like {'mds:tool': 'FFE6E6', 'mds:recipe': 'E6FFE6'}
        - Updates ONTO_CORE_CATEGORIES with prefixed category names
    """
    global CATEGORY_COLORS, ONTO_CORE_CATEGORIES
    
    # Clear existing mappings
    CATEGORY_COLORS.clear()
    colors = list(LIGHT_COLORS.values())

    # Convert categories to prefixed form (e.g. 'mds:tool')
    prefixed_categories = {get_prefixed_name(category) for category in categories}
    
    # Assign colors cycling through palette
    for i, category in enumerate(prefixed_categories):
        color_idx = i % len(colors)
        CATEGORY_COLORS[category] = colors[color_idx]

    # Update core categories set
    ONTO_CORE_CATEGORIES.clear()
    ONTO_CORE_CATEGORIES.update(prefixed_categories)


def get_prefixed_name(uri: Union[str, URIRef]) -> str:
    """
    Converts a full URI to its corresponding prefixed form using the global namespace mappings.

    Algorithm:
    1. Convert the URIRef to a string if needed.
    2. Iterate over NAMESPACE_MAP to find a prefix whose namespace is a prefix of the given URI.
    3. If found, return prefix:LocalName. Otherwise, return the original URI string.

    Args:
        uri (Union[str, URIRef]): The URI to convert.

    Returns:
        str: Prefixed form of the URI (e.g. 'mds:SampleSize').
    """
    for prefix, ns in NAMESPACE_MAP.items():
        if str(uri).startswith(ns):
            return f"{prefix}:{str(uri)[len(ns):]}"
    return str(uri)


###############################################################################
# Core Ontology Analysis Functions
###############################################################################

def find_leaf_nodes(lowest_level_ontology_path: str) -> Set[URIRef]:
    """
    Identifies leaf nodes in the lowest-level ontology. Leaf nodes are classes that do not serve 
    as a superclass of any other class within the MDS namespace.

    Algorithm:
    1. Parse the lowest-level ontology and build an RDF graph.
    2. Gather all classes (subjects of RDFS.subClassOf) and their superclasses.
    3. Classes that never appear as an RDFS.subClassOf object are considered leaf nodes.

    Time Complexity:
    O(N + E) where N is the number of classes and E the number of subclass relations.

    Space Complexity:
    O(N) for storing classes and relationships.

    Args:
        lowest_level_ontology_path (str): Path to the low-level ontology (.ttl file).

    Returns:
        Set[URIRef]: A set of URIs representing leaf classes.
    """
    graph = Graph()
    graph.parse(lowest_level_ontology_path, format='ttl')

    # Collect MDS superclasses
    superclasses = set(
        obj for obj in graph.objects(None, RDFS.subClassOf) if str(obj).startswith(str(MDS))
    )
    # Collect MDS classes (appear as subjects of RDFS.subClassOf)
    all_classes = set(
        subj for subj in graph.subjects(RDFS.subClassOf, None) if str(subj).startswith(str(MDS))
    )
    all_classes.update(superclasses)

    # Leaf nodes are classes not appearing as superclass of any other class
    leaf_nodes = all_classes - superclasses
    return leaf_nodes


def get_top_level_terms_from_combined(combined_ontology_path: str) -> Set[str]:
    """
    Derives top-level categories directly from the combined ontology. This removes the need for a separate 
    top-level ontology file.

    A top-level category is defined as a class that appears as a 'broader' concept (object of SKOS.broader) 
    but does not appear as a narrower concept for any other class within the MDS namespace. 
    If no such classes are found, we consider classes with no broader relations as top-level.

    Algorithm:
    1. Parse the combined ontology.
    2. For all triples (narrower SKOS.broader broader), record narrower and broader classes.
    3. Top-level categories are those that appear as broader but never as narrower.
    4. If none found this way, fallback to classes that never appear as narrower at all.

    Time Complexity:
    O(N + E) where N is number of classes and E is number of SKOS.broader relationships.

    Space Complexity:
    O(N) for storing class sets and relationships.

    Args:
        combined_ontology_path (str): Path to the combined MDS ontology (.ttl file).

    Returns:
        Set[str]: A set of URIs for top-level category classes.
    """
    graph = Graph()
    graph.parse(combined_ontology_path, format='ttl')

    narrower_classes = set()
    broader_classes = set()

    # Collect SKOS.broader relationships
    for subj in graph.subjects(SKOS.broader, None):
        if str(subj).startswith(str(MDS)):
            for obj in graph.objects(subj, SKOS.broader):
                if str(obj).startswith(str(MDS)):
                    narrower_classes.add(str(subj))
                    broader_classes.add(str(obj))

    # Top-level: appear as broader but not as narrower
    top_level_categories = {c for c in broader_classes if c not in narrower_classes}

    if not top_level_categories:
        # Fallback: Consider classes that never appear as narrower at all
        all_mds_classes = {str(c) for c in graph.subjects(RDF.type, OWL.Class) if str(c).startswith(str(MDS))}
        # potential tops are those not in narrower_classes
        potential_tops = all_mds_classes - narrower_classes
        top_level_categories = potential_tops

    return top_level_categories


def classify_leaf_nodes(combined_ontology_path: str, 
                        leaf_nodes: Set[URIRef], 
                        top_level_terms: Set[str]) -> Tuple[Dict[str, List[str]], List[str]]:
    """
    Classifies each leaf node into a top-level category by traversing upward along `rdfs:subClassOf` and 
    `skos:broader` relationships until a known top-level category is found.

    Algorithm:
    1. Parse the combined ontology into a graph.
    2. For each leaf node, recursively follow `rdfs:subClassOf` and `skos:broader` upwards.
    3. If a top-level category is reached, classify the leaf under that category.
    4. If no top-level category is found, mark the leaf as missing.

    This uses memoization to avoid repeated traversals of the same class.

    Time Complexity:
    O(N + E) with memoization, where N is number of nodes and E is number of edges.

    Space Complexity:
    O(N) for memoization and classification structures.

    Args:
        combined_ontology_path (str): Path to the combined ontology (.ttl file).
        leaf_nodes (Set[URIRef]): Set of leaf node URIs identified from the low-level ontology.
        top_level_terms (Set[str]): Set of URIs representing top-level categories.

    Returns:
        Tuple[Dict[str, List[str]], List[str]]:
            - Dictionary mapping top-level category URIs to a list of leaf node URIs.
            - List of URIs for leaf nodes that couldn't be mapped.
    """
    graph = Graph()
    graph.parse(combined_ontology_path, format='ttl')

    classification = {}
    memoization = {}
    missing_top_terms = []

    def trace_to_top(term_uri, visited=None):
        if visited is None:
            visited = set()

        term_str = str(term_uri)
        # Check if current term is already known top-level
        if term_str in top_level_terms:
            memoization[term_uri] = term_uri
            return term_uri

        # Avoid cycles
        if term_uri in visited:
            return None
        visited.add(term_uri)

        # Traverse via rdfs:subClassOf
        for superclass in graph.objects(term_uri, RDFS.subClassOf):
            top_term = trace_to_top(superclass, visited)
            if top_term:
                memoization[term_uri] = top_term
                return top_term

        # Traverse via skos:broader
        for broader_term in graph.objects(term_uri, SKOS.broader):
            top_term = trace_to_top(broader_term, visited)
            if top_term:
                memoization[term_uri] = top_term
                return top_term

        memoization[term_uri] = None
        return None

    # Classify each leaf node
    for leaf in leaf_nodes:
        top_term = trace_to_top(leaf)
        if top_term:
            top_term_str = str(top_term)
            leaf_str = str(leaf)
            classification.setdefault(top_term_str, []).append(leaf_str)
        else:
            missing_top_terms.append(str(leaf))

    return classification, missing_top_terms


def get_classification(lowest_level_ontology_path: str, 
                       combined_ontology_path: str) -> Tuple[Dict[str, List[str]], List[str]]:
    """
    High-level function that coordinates:
    1. Finding leaf nodes from the low-level ontology.
    2. Identifying top-level categories directly from the combined ontology.
    3. Classifying leaf nodes under these top-level categories.
    4. Updating category colors and converting URIs to prefixed forms.

    Args:
        lowest_level_ontology_path (str): Path to the lowest-level MDS ontology (.ttl file).
        combined_ontology_path (str): Path to the combined MDS ontology (.ttl file).

    Returns:
        Tuple[Dict[str, List[str]], List[str]]:
            - classification_prefixed: A dictionary with prefixed category URIs as keys and lists of prefixed leaf nodes as values.
            - missing_top_terms_prefixed: A list of prefixed URIs for terms that couldn't be mapped.
    """
    # Step 1: Identify leaf nodes
    leaf_nodes = find_leaf_nodes(lowest_level_ontology_path)

    # Step 2: Identify top-level categories from combined ontology
    top_level_terms = get_top_level_terms_from_combined(combined_ontology_path)

    # Step 3: Classify leaf nodes
    classification, missing_top_terms = classify_leaf_nodes(combined_ontology_path, leaf_nodes, top_level_terms)

    # Step 4: Update category colors based on discovered categories
    update_category_colors(set(classification.keys()))

    # Convert URIs to prefixed names and strip prefixes from terms
    classification_prefixed = {}
    for category_uri, leaf_uris in classification.items():
        category_prefixed = get_prefixed_name(category_uri)
        leaf_terms = [get_prefixed_name(uri).split(':')[1] for uri in leaf_uris]
        classification_prefixed[category_prefixed] = leaf_terms
        
        # Ensure category color exists
        if category_prefixed not in CATEGORY_COLORS:
            print(f"Warning: No color assigned for category {category_prefixed}")

    missing_top_terms_prefixed = [get_prefixed_name(uri) for uri in missing_top_terms]
    # print(classification_prefixed)
    return classification_prefixed, missing_top_terms_prefixed
