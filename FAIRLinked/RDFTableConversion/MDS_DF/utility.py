import os
import json
import re
from pyld import jsonld
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF, OWL, RDFS, DCTERMS, SKOS
from urllib.parse import urlparse
from ... import helper_data as helper_data
import hashlib
import requests
from importlib import resources
import difflib

def load_licenses():
    with resources.files(helper_data).joinpath("licenseinfo.json").open() as f:
        spdx_data = json.load(f)
    return spdx_data

_CACHED_UNITS = None

def load_units():
    """
    Optimized unit loader using single-pass regex and global caching.
    """
    global _CACHED_UNITS
    
    # 1. Return cached data if already loaded
    if _CACHED_UNITS is not None:
        return _CACHED_UNITS

    try:
        # 2. Access the file via importlib.resources
        with resources.files(helper_data).joinpath("qudt_unit.ttl").open() as f:
            content = f.read()

        # 3. Optimized Single-Pass Regex: 
        # Captures the unit name AND its associated block in one go
        # This prevents scanning the entire file for every unit
        unit_blocks = re.findall(
            r'unit:([A-Z0-9_\-]+)\s*\n(.*?)(?=\nunit:|$)', 
            content, 
            re.DOTALL
        )
        
        unit_details = {}
        
        for unit_name, block in unit_blocks:
            # 4. Extract details ONLY from the local block string
            # This is significantly faster than searching the whole file
            
            # Extract symbol
            symbol_match = re.search(r'qudt:symbol\s+"([^"]+)"', block)
            symbol = symbol_match.group(1) if symbol_match else None
            
            # Extract label(s)
            label_matches = re.findall(r'rdfs:label\s+"([^"]+)"(?:@\w+)?', block)
            label = label_matches[0] if label_matches else unit_name
            
            # Extract description
            desc_match = re.search(r'dcterms:description\s+"([^"]+)"', block)
            description = desc_match.group(1) if desc_match else None
            
            # Extract UCUM code
            ucum_match = re.search(r'qudt:ucumCode\s+"([^"]+)"', block)
            ucum_code = ucum_match.group(1) if ucum_match else None
            
            # Extract conversion multiplier
            conv_match = re.search(r'qudt:conversionMultiplier\s+([\d.E\-+]+)', block)
            conversion = conv_match.group(1) if conv_match else None
            
            unit_details[unit_name] = {
                'name': unit_name,
                'label': label,
                'symbol': symbol,
                'ucum_code': ucum_code,
                'conversion_multiplier': conversion,
                'description': description[:100] + '...' if description and len(description) > 100 else description
            }
        
        # 5. Store in global cache for future calls
        _CACHED_UNITS = unit_details
        return _CACHED_UNITS

    except Exception as e:
        print(f"⚠️ Unit loading failed: {e}. Returning empty dictionary.")
        return {}


def hash6(s):
    """
    Takes any string and returns a 6-digit number (100000-999999).
    
    Args:
        s: Input string to hash
        
    Returns:
        int: A 6-digit number between 100000 and 999999
    """
    # Create a hash of the string using SHA-256
    hash_obj = hashlib.sha256(s.encode('utf-8'))
    
    # Convert hash to integer
    hash_int = int(hash_obj.hexdigest(), 16)
    
    # Map to 6-digit range (100000-999999)
    six_digit = (hash_int % 900000) + 100000
    
    return six_digit

def resolve_predicate(key, ontology_graph):
    """
    Resolves a given key into a full RDF predicate URI and determines its property type
    (object or datatype) within a provided ontology graph.

    The function accepts either a full IRI (e.g. ``http://example.org/ontology#hasMaterial``)
    or a CURIE (e.g. ``ex:hasMaterial``).  It first checks whether the key is a valid absolute IRI.
    If not, it attempts to expand the key as a CURIE using the namespace manager attached to the
    supplied RDFLib ontology graph.  If neither expansion succeeds, the function returns
    ``(None, None)``.

    Once a valid predicate URI is obtained, the function inspects the ontology graph to determine
    whether the predicate is an ``owl:ObjectProperty`` or an ``owl:DatatypeProperty``.  If neither
    type is declared in the ontology, the label type is returned as ``None``.

    Parameters
    ----------
    key : str
        Predicate identifier to resolve.  Can be a full IRI (e.g. ``http://...``) or a CURIE
        (e.g. ``ex:hasMaterial``).

    ontology_graph : rdflib.Graph
        RDFLib graph object representing the ontology within which the predicate should be
        resolved.  The graph must have a properly configured ``namespace_manager`` to expand
        CURIEs.

    Returns
    -------
    tuple
        A 2-tuple of the form ``(predicate_uri, label_type)`` where:
        - ``predicate_uri`` is an ``rdflib.term.URIRef`` representing the resolved predicate IRI,
          or ``None`` if the key could not be resolved.
        - ``label_type`` is a string describing the property type:
          ``"Object Property"``, ``"Datatype Property"``, or ``None`` if no type match was found.
    """
    
    # try full iri
    parsed = urlparse(key)
    if parsed.scheme and parsed.netloc:
        pred_uri = URIRef(key)
    else:
        # try curie
        try:
            pred_uri = ontology_graph.namespace_manager.expand_curie(key)
        except ValueError:
            return None, None  # Not a valid IRI or CURIE → skip

    # determine type
    if (pred_uri, RDF.type, OWL.ObjectProperty) in ontology_graph:
        label_type = "Object Property"
    elif (pred_uri, RDF.type, OWL.DatatypeProperty) in ontology_graph:
        label_type = "Datatype Property"
    else:
        label_type = None

    return pred_uri, label_type


def write_license_triple(output_folder: str, base_uri: str, license_id: str):
    """
    Creates a compact JSON-LD file defining a single RDF triple that links a dataset to its license.

    This function generates a minimal JSON-LD graph of the form:
        mds:Dataset dcterms:license <SPDX_URI>

    If a short SPDX identifier (e.g. "MIT", "CC-BY-4.0") is provided, the function verifies that the
    identifier exists in the official SPDX license list (`licenses.json`, bundled with the package)
    and converts it to its canonical SPDX URI (e.g.
    `https://spdx.org/licenses/MIT.html`).  If a full URI beginning with "http" is supplied, the URI
    is used as-is.

    The resulting triple is serialized to a compact JSON-LD file named
    ``dataset_license.jsonld`` in the specified output folder.  The JSON-LD document includes a
    top-level ``@context`` containing compact namespace prefixes for ``mds`` and ``dcterms``.

    Parameters
    ----------
    output_folder : str
        Path to the directory where the output JSON-LD file will be written.  The directory is
        created if it does not exist.

    base_uri : str
        Base namespace URI of the MDS ontology.  The function appends a fragment (“#”) and uses
        ``mds:Dataset`` as the subject IRI of the triple.

    license_id : str
        SPDX short identifier (e.g., "MIT", "CC-BY-4.0") OR full license URI.  Short identifiers are
        validated against the official SPDX license list before being converted into full URIs.

    Outputs
    -------
    dataset_license.jsonld : file
        A JSON-LD file written to ``output_folder`` with the structure:

        ```json
        {
          "@context": {
            "mds": "https://cwrusdle.bitbucket.io/mds/",
            "dcterms": "http://purl.org/dc/terms/"
          },
          "@id": "mds:Dataset",
          "dcterms:license": {
            "@id": "https://spdx.org/licenses/MIT.html"
          }
        }
        ```

    """

    # --- 1️⃣ Validate and convert SPDX short ID to full URI ---
    if not license_id.startswith("http"):
        # Load SPDX license list
        

        spdx_data = load_licenses()

        valid_ids = {lic["licenseId"] for lic in spdx_data["licenses"]}

        # Check if the provided short ID is valid
        if license_id not in valid_ids:
            raise ValueError(
                f"Invalid SPDX license ID '{license_id}'.\n"
                f"Please use one from https://spdx.org/licenses/."
            )

        license_uri = f"https://spdx.org/licenses/{license_id}.html"

    else:
        # Full URI provided; assume it's valid
        license_uri = license_id


    # Create RDF graph
    g = Graph()
    MDS = Namespace(base_uri)
    g.bind("mds", MDS)
    g.bind("dcterms", DCTERMS)

    g.add((MDS.Dataset, DCTERMS.license, URIRef(license_uri)))

    # Serialize to JSON-LD (expanded form)
    jsonld_data = json.loads(g.serialize(format="json-ld"))

    # Define desired context
    context = {
        "mds": str(MDS),
        "dcterms": str(DCTERMS)
    }

    # Compact using pyld to get CURIEs instead of full IRIs
    compacted = jsonld.compact(jsonld_data, context)

    # Write to file
    json_path = os.path.join(output_folder, "dataset_license.jsonld")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(compacted, f, indent=2)



def normalize(text):
    """
    Normalize a text string by converting it to lowercase and removing non-alphanumeric characters.

    Args:
        text (str): Input text to normalize.

    Returns:
        str: Normalized string.
    """

    text = text.lower().replace(" ", "_")
    return re.sub(r'[^a-zA-Z0-_.]', '', text.lower())

def normalize_iri(text):
    """
    Normalize a text string by converting it to lowercase and removing non-alphanumeric characters.

    Args:
        text (str): Input text to normalize.

    Returns:
        str: Normalized string.
    """

    text = text.lower().replace(" ", "_")
    return re.sub(r'[^a-zA-Z0-_./]', '', text.lower())

def get_local_name(uri):
        uri_str = str(uri)
        # Split by / or # and get the last part
        if '/' in uri_str:
            return uri_str.split('/')[-1]
        elif '#' in uri_str:
            return uri_str.split('#')[-1]
        return uri_str

def extract_terms_from_ontology(ontology_graph):
    """
    Extract terms from an RDF graph representing an OWL ontology.

    Args:
        ontology_graph (rdflib.Graph): The ontology RDF graph.

    Returns:
        list[dict]: A list of dictionaries containing term IRIs, original labels, and normalized labels.
    """
    MDS = Namespace("https://cwrusdle.bitbucket.io/mds/")
    
    terms = []
    for s in ontology_graph.subjects(RDF.type, OWL.Class):
        
        # Get both altLabels and rdfs:labels
        labels = list(ontology_graph.objects(s, SKOS.altLabel)) + list(ontology_graph.objects(s, RDFS.label))
        # Get definitions
        term_definitions = list(ontology_graph.objects(s, SKOS.definition))
        definition = str(term_definitions[0]) if term_definitions else ""
        study_stage = list(ontology_graph.objects(s, MDS.hasStudyStage))
        for label in labels:
            label_str = str(label).strip()
            terms.append({
                "iri": str(s),
                "label": label_str,
                "normalized": normalize(label_str),
                "definition": definition,
                "study_stage": study_stage
            })
    return terms


def find_best_match(column, ontology_terms):
    """
    Find the best matching ontology term for a given column name.

    Args:
        column (str): The name of the column from the CSV file.
        ontology_terms (list[dict]): List of extracted ontology terms.

    Returns:
        dict or None: The best-matching ontology term, or None if no good match is found.
    """
    norm_col = normalize(column)

    # First, try exact normalized match
    matches = [term for term in ontology_terms if term["normalized"] == norm_col]
    if matches:
        return matches[0]

    # Otherwise, find close match using difflib
    all_norm = [term["normalized"] for term in ontology_terms]
    close_matches = difflib.get_close_matches(norm_col, all_norm, n=1, cutoff=0.8)

    if close_matches:
        match_norm = close_matches[0]
        return next(term for term in ontology_terms if term["normalized"] == match_norm)

    return None

def extract_qudt_units(url="https://qudt.org/vocab/unit/"):
    """
    Extract all units from the QUDT ontology programmatically.
    
    Args:
        url: The URL of the QUDT unit vocabulary
    
    Returns:
        Dictionary containing unit information
    """
    print(f"Fetching QUDT ontology from: {url}")
    
    try:
        # Fetch the ontology data
        response = requests.get(url, headers={'Accept': 'text/turtle'})
        response.raise_for_status()
        content = response.text
        
        print(f"Successfully fetched {len(content)} characters of data\n")
        
        # Extract units using regex patterns
        # Pattern to match unit definitions: unit:UNIT_NAME
        unit_pattern = r'unit:([A-Z0-9_\-]+)\s*\n\s*a\s+qudt:(?:Unit|DerivedUnit)'
        
        # Find all unit names
        units = re.findall(unit_pattern, content)
        
        # Dictionary to store unit details
        unit_details = {}
        
        # For each unit, extract additional information
        for unit_name in units:
            # Create a pattern to find the unit's definition block
            unit_block_pattern = rf'unit:{re.escape(unit_name)}\s*\n(.*?)(?=\nunit:|$)'
            match = re.search(unit_block_pattern, content, re.DOTALL)
            
            if match:
                unit_block = match.group(1)
                
                # Extract symbol
                symbol_match = re.search(r'qudt:symbol\s+"([^"]+)"', unit_block)
                symbol = symbol_match.group(1) if symbol_match else None
                
                # Extract label(s)
                label_matches = re.findall(r'rdfs:label\s+"([^"]+)"(?:@\w+)?', unit_block)
                label = label_matches[0] if label_matches else unit_name
                
                # Extract description
                desc_match = re.search(r'dcterms:description\s+"([^"]+)"', unit_block)
                description = desc_match.group(1) if desc_match else None
                
                # Extract UCUM code
                ucum_match = re.search(r'qudt:ucumCode\s+"([^"]+)"', unit_block)
                ucum_code = ucum_match.group(1) if ucum_match else None
                
                # Extract conversion multiplier
                conv_match = re.search(r'qudt:conversionMultiplier\s+([\d.E\-+]+)', unit_block)
                conversion = conv_match.group(1) if conv_match else None
                
                unit_details[unit_name] = {
                    'name': unit_name,
                    'label': label,
                    'symbol': symbol,
                    'ucum_code': ucum_code,
                    'conversion_multiplier': conversion,
                    'description': description[:100] + '...' if description and len(description) > 100 else description
                }
        
        return unit_details
        
    except requests.RequestException as e:
        print(f"Error fetching data for units: {e}")
        return {}

def extract_quantity_kinds():
    try:
        url = "https://qudt.org/vocab/quantitykind/"
        # Fetch the ontology data
        response = requests.get(url, headers={'Accept': 'text/turtle'})
        response.raise_for_status()
        g = Graph()
        g.parse(data=response.text, format='turtle')
        predicate = URIRef("http://qudt.org/schema/qudt/applicableUnit")
        kinds = {}
        
        for subject in g.subjects(predicate=predicate):
            # Get all objects for this subject-predicate pair
            s= normalize(get_local_name(subject))
            kinds[s] = [get_local_name(obj) for obj in g.objects(subject=subject, predicate=predicate)]
        return kinds
    except Exception as e:
        print(e)
        return {}




def prompt_for_missing_fields(col, unit, study_stage, ontology_graph, units):
    print(f"\n-- Getting metadata for column: {col} --")
    print("(Type 'skip' or 'exit' to default to UNITLESS)")
    
    # --- Part 1: Unit Selection Loop ---
    while True:
        user_input = input(f"Search unit/UCUM for '{col}': ").strip()
        
        # If user wants to stop or skip, default to UNITLESS and break the loop
        if user_input.lower() in ['exit', 'quit', 'stop', 'skip', '']:
            unit = "UNITLESS"
            break

        # Search Logic
        matches = []
        for key, details in units.items():
            if (user_input == details.get('ucum_code') or 
                user_input.lower() == details.get('label').lower()):
                matches.append(key)

        if len(matches) == 1:
            unit = matches[0]
            break # Found it! Move on to next fields
        
        elif len(matches) > 1:
            print("\nMultiple matches found. Select a number or type 'back':")
            for i, m in enumerate(matches, 1):
                print(f"  {i}. {units[m]['label']} ({m})")
            
            choice = input("> ")
            if choice.lower() == 'back':
                continue 
            if choice.isdigit() and 1 <= int(choice) <= len(matches):
                unit = matches[int(choice) - 1]
                break
        else:
            print(f"No match for '{user_input}'. Try again or type 'exit' to use UNITLESS.")

    # --- Part 2: Study Stage ---
    valid_study_stages = [
        "Synthesis", "Formulation", "Material Processing", "Sample", 
        "Tool", "Recipe", "Result", "Analysis", "Modeling", ""
    ]
    norm_study_stages = [normalize(ss) for ss in valid_study_stages]

    # Initial check
    if not study_stage or normalize(study_stage) not in norm_study_stages:
        print("\nPlease enter a valid study stage or press 'enter' to skip: ")
        for ss in valid_study_stages:
            if ss: 
                print(f" - {ss}")
        
        study_stage = input("Stage: ")
        
        # Keep asking until valid
        while normalize(study_stage) not in norm_study_stages:
            study_stage = input("Invalid stage. Please try again (or 'enter' to skip): ")

    # Normalize back to the pretty-print version
    study_stage = valid_study_stages[norm_study_stages.index(normalize(study_stage))]

    # --- Part 3: Notes ---
    notes = input("Please enter notes or press 'Enter' to skip: ")

    return unit, study_stage, notes

def get_license():
    return input("Please enter license")

def get_curie(iri, bindings):
        """
        Converts a full IRI to a CURIE using the provided bindings.
        Defaults to 'obo:BFO_0000001' if no match is found
        """
        if not iri:
            return "obo:BFO_0000001"

        best_prefix = None
        best_uri = ""

        for prefix, uri in bindings.items():
            # Skip JSON-LD keywords
            if "@" in prefix:
                continue
            
            # Check if the IRI starts with this namespace URI
            if iri.startswith(str(uri)):
                # We want the longest matching URI for accuracy
                if len(str(uri)) > len(best_uri):
                    best_prefix = prefix
                    best_uri = str(uri)

        if best_prefix:
            # Remove the URI part and join with the prefix
            fragment = iri[len(best_uri):].lstrip('#/')
            return f"{best_prefix}:{fragment}"

        return "obo:BFO_0000001"