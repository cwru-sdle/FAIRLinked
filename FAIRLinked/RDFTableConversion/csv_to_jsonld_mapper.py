import pandas as pd
import json
import re
import os
import difflib
import rdflib
from datetime import datetime
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS, split_uri
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import sys
from fuzzysearch import find_near_matches
import requests
import ast

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




def prompt_for_missing_fields(col,unit, study_stage, ontology_graph, units):
    print(f"--Enter terms for {col} --")
    if(unit not in units):
        userinput = normalize(input("Please select the type of quantity (eg. length, density, unitless, etc) or hit 'enter' to skip:  "))
        if(userinput in ["unitless", ""]):
            match userinput:
                case "unitless":
                    unit = "UNITLESS"
                case "":
                    unit = "UNITLESS"
        else:
            kinds = extract_quantity_kinds()
            ty = userinput
            while ty not in kinds and ty:
                ty = normalize(input("Please enter the type of quantity this is or hit 'enter' to skip: "))

            if not ty:
                unit = "UNITLESS"
            else:
                print("Valid Units: ",kinds[ty])
                while ( unit not in kinds[ty]):
                    unit = input("Please enter valid units: ")
    


    valid_study_stages = [
        "Synthesis", "Formulation", "Material Processing","Sample", 
        "Tool", "Recipe", "Result", "Analysis", "Modeling" ]

    norm_study_stages = [normalize(ss) for ss in valid_study_stages]

    if(normalize(study_stage) not in norm_study_stages):
        print("Please enter a valid study stage from options below: ")
        for ss in valid_study_stages:
            print(ss)
        study_stage = input("Please enter valid study stage: ")
    while(normalize(study_stage) not in norm_study_stages):
        study_stage = input("Please enter valid study stage: ")
        
    study_stage = valid_study_stages[norm_study_stages.index(normalize(study_stage))]

    notes = input("Please enter notes: ")

    return unit, study_stage, notes

def get_license():
    return input("Please enter license")

def jsonld_template_generator(csv_path, ontology_graph, output_path, matched_log_path, unmatched_log_path):
    """
    Use a CSV file into a JSON-LD template that user can fill out column metadata.

    Args:
        csv_path (str): Path to the CSV file to generate JSON-LD template.
        ontology_graph (rdflib.Graph): The ontology RDF graph for matching terms.
        output_path (str): Path to write the resulting JSON-LD file.
        matched_log_path (str): Path to write the log of columns that matched the ontology.
        unmatched_log_path (str): Path to write the log of columns that can't be found in the ontology.
    """
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    ontology_terms = extract_terms_from_ontology(ontology_graph)

    # Load all possible bindings 
    bindings_dict = {prefix: str(namespace) for prefix, namespace in ontology_graph.namespaces()}

    matched_log = []
    unmatched_log = []
    bindings = {}


    # Construct the base JSON-LD structure
    jsonld = {
        "@context": {
            "qudt": "http://qudt.org/schema/qudt/",
            "mds": "https://cwrusdle.bitbucket.io/mds#",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", 
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#", 
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.example.org/",
            "prov": "http://www.w3.org/ns/prov#",
            "dcterms": "http://purl.org/dc/terms/"      
        },
        "@graph": []
    }
    units = extract_qudt_units()
    # Process each column and attempt to match it to ontology terms
    for col in columns:
        if col == "__source_file__" or col == "__Label__" or col == "__rowkey__":
            continue
        typ = df.loc[0,col]

        match = find_best_match(col, ontology_terms)
        if(pd.isna(typ) or ":" not in typ):# if no type was explicitily included in csv
           
            #get iri from closest match
            iri_fragment = str(match["iri"]).split("/")[-1].split("#")[-1] if match else normalize(col)

            # Get base iri
            iri_str = str(match["iri"]) if match else None
            binding =""
            study_stage = ""
            definition = "Definition not available"
            if iri_str:
                last_slash = iri_str.rfind("/")
                last_hash = iri_str.rfind("#")
                split_pos = max(last_slash, last_hash)
                iri_base = iri_str[:split_pos + 1] if split_pos != -1 else iri_str
                binding = next((k for k, v in bindings_dict.items() if v == iri_base), "mds")
                
                #add binding to list of contexts
                if(binding not in bindings):
                    bindings[binding] = bindings_dict[binding]

                definition = str(match["definition"]) if match else "Definition not available"
                study_stage = match["study_stage"][0].value if match else "Study stage information not available"
               
        else: #csv included type:
            binding, iri_fragment = typ.split(":")
            if(binding == "mds"):
                #if term in mds ontology, get study stage and def from ontologyt
                definition = str(match["definition"]) if match else "Definition not available"
                study_stage = match["study_stage"][0].value if match else "Study stage information not available"
            else:
                definition = "Definition not available"
                study_stage = df.loc[2,col] #try to get study stage from csv
                if pd.isna(study_stage): study_stage =  "Study stage information not available"

        # try get units
        un = df.loc[1,col]

        if(not pd.isna(un)):
            try:
                un = ast.literal_eval(un).get('@id', "").split(":")[1]
            except :
                pass
            
        if match:
            matched_log.append(f"{col} => {iri_fragment}")

        else:
            unmatched_log.append(col)

        import importlib.util

        
        unit, study, notes = prompt_for_missing_fields(iri_fragment,un, study_stage,ontology_graph,units)
        

        if(binding == ""):
            binding = "mds"

        if(binding not in bindings):
                    bindings[binding] = bindings_dict[binding]
         
        entry = {
            "@id": f"{binding}:{iri_fragment}",
            "@type": f"{binding}:{iri_fragment}",
            "skos:altLabel": col,
            "skos:definition": definition,
            "qudt:value": [{"@value": ""}],
            "qudt:hasUnit": {"@id": f"unit:{unit}"},
            "prov:generatedAtTime": {
                "@value": datetime.now().astimezone().isoformat(),
                "@type": "xsd:dateTime"
            },
            "skos:note": {
                "@value": f"{notes}",
                "@language": "en"
            },
            "mds:hasStudyStage": study
        }
        jsonld["@graph"].append(entry)

    # Ensure output directories exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.makedirs(os.path.dirname(matched_log_path), exist_ok=True)
    os.makedirs(os.path.dirname(unmatched_log_path), exist_ok=True)

    # Add contexts
    jsonld["@context"].update({
        "unit": "https://qudt.org/vocab/unit/"
    })
    for i in bindings:
        jsonld["@context"].update({
            i: bindings[i]
        })

    # Write JSON-LD
    with open(output_path, "w") as f:
        json.dump(jsonld, f, indent=2)

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
    jsonld_template_generator(csv_path=args.csv_path, ontology_graph=ontology_graph, output_path=args.output_path, matched_log_path=matched_path, unmatched_log_path=unmatched_path)
    
