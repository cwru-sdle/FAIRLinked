import pandas as pd
import json
import re
import os
import difflib
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS
from ..InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import requests
from .MDS_DF.main import MatDatSciDf

def normalize(text):
    """
    Normalize a text string by converting it to lowercase and removing non-alphanumeric characters.

    Args:
        text (str): Input text to normalize.

    Returns:
        str: Normalized string.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

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

def jsonld_template_generator(csv_path, ontology_graph, output_path, matched_log_path, unmatched_log_path, skip_prompts=False):
    """
    Use a CSV file into a JSON-LD template that user can fill out column metadata.

    Args:
        csv_path (str): Path to the CSV file to generate JSON-LD template.
        ontology_graph (rdflib.Graph): The ontology RDF graph for matching terms.
        output_path (str): Path to write the resulting JSON-LD file.
        matched_log_path (str): Path to write the log of columns that matched the ontology.
        unmatched_log_path (str): Path to write the log of columns that can't be found in the ontology.
        skip_prompts (bool): Allow users to skip metadata prompts
    """
    df = pd.read_csv(csv_path)
    mds_df = MatDatSciDf(
            df = df,
            metadata_rows=True,
            ontology_graph=ontology_graph
            )

    metadata_template, matched_log, unmatched_log = mds_df.template_generator(skip_prompts=skip_prompts)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.makedirs(os.path.dirname(matched_log_path), exist_ok=True)
    os.makedirs(os.path.dirname(unmatched_log_path), exist_ok=True)

    # Write JSON-LD
    with open(output_path, "w") as f:
        json.dump(metadata_template, f, indent=2)

    # Write matched log
    with open(matched_log_path, "w") as f:
        f.write("\n".join(matched_log))

    # Write unmatched log (remove duplicates with set)
    with open(unmatched_log_path, "w") as f:
        f.write("\n".join(sorted(set(unmatched_log))))  # BUG FIX: previously had stray '-' before 'fix'

def jsonld_temp_gen_interface(args):

    print(args.ontology_path)
    if args.ontology_path == "default":
        ontology_graph = Graph()
        ontology_graph = load_mds_ontology_graph()       
    else:
        ontology_graph = Graph()
        ontology_graph.parse(source=args.ontology_path)

    matched_path = os.path.join(args.log_path, "matched.txt")
    unmatched_path = os.path.join(args.log_path, "unmatched.txt")
    jsonld_template_generator(csv_path=args.csv_path, 
                            ontology_graph=ontology_graph, 
                            output_path=args.output_path, 
                            matched_log_path=matched_path, 
                            unmatched_log_path=unmatched_path, 
                            skip_prompts=args.skip_prompts)
    
