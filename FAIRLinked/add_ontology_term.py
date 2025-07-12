#!/usr/bin/env python3
"""
Script to interactively add a new term to an existing ontology in Turtle (TTL) format.

It prompts the user for required metadata such as label, parent class, value type, and unit,
and appends the formatted RDF triples to a new file (original name suffixed with _rxl895).

This script is useful for ontology developers working on modular additions while preserving
the integrity of the original ontology file.
"""

import os
import sys
from datetime import datetime

def get_term_details():
    """
    Prompt the user to input details for a new ontology term.

    Returns:
        dict: A dictionary containing term_name, label, parent_class, definition,
              value_type, and unit.
    Raises:
        ValueError: If required inputs are left empty or invalid selections are made.
    """
    print("\nPlease provide the following details for the new term:")
    
    term_name = input("Enter the term name (e.g., 'NewDetector'): ").strip()
    if not term_name:
        raise ValueError("Term name cannot be empty")
    
    label = input("Enter the label (human-readable name): ").strip()
    if not label:
        raise ValueError("Label cannot be empty")
    
    print("\nAvailable parent classes:")
    parent_classes = [
        "mds:Tool", "mds:Sample", "mds:Recipe", "mds:Model", 
        "mds:Result", "mds:Identifier", "mds:ValueObject"
    ]
    for i, cls in enumerate(parent_classes, 1):
        print(f"{i}. {cls}")
    
    while True:
        try:
            parent_choice = input("\nSelect parent class number: ").strip()
            parent_class = parent_classes[int(parent_choice) - 1]
            break
        except (ValueError, IndexError):
            print("Invalid selection. Please enter a number between 1 and", len(parent_classes))
    
    definition = input("Enter the definition: ").strip()
    if not definition:
        raise ValueError("Definition cannot be empty")
    
    print("\nSelect value type (if applicable):")
    value_types = [
        "xsd:string", "xsd:float", "xsd:integer", "xsd:dateTime",
        "rdf:Seq", "None"
    ]
    for i, vtype in enumerate(value_types, 1):
        print(f"{i}. {vtype}")
    
    while True:
        try:
            value_choice = input("\nSelect value type number: ").strip()
            value_type = value_types[int(value_choice) - 1]
            break
        except (ValueError, IndexError):
            print("Invalid selection. Please enter a number between 1 and", len(value_types))
    
    unit = None
    if value_type in ["xsd:float", "xsd:integer"]:
        print("\nSelect unit (if applicable):")
        units = [
            "qudt:MicroM", "qudt:MilliM", "qudt:ANGSTROM", 
            "qudt:KiloEV", "qudt:Atmosphere", "qudt:Bar",
            "qudt:Pascal", "qudt:Kelvin", "qudt:Celsius",
            "qudt:Second", "qudt:Minute", "qudt:Hour",
            "qudt:Gram", "qudt:Kilogram", "qudt:Newton",
            "qudt:Joule", "qudt:Watt", "None"
        ]
        for i, u in enumerate(units, 1):
            print(f"{i}. {u}")
        
        while True:
            try:
                unit_choice = input("\nSelect unit number: ").strip()
                unit = units[int(unit_choice) - 1]
                break
            except (ValueError, IndexError):
                print("Invalid selection. Please enter a number between 1 and", len(units))
    
    return {
        "term_name": term_name,
        "label": label,
        "parent_class": parent_class,
        "definition": definition,
        "value_type": value_type,
        "unit": unit
    }

def format_term(term_details):
    """
    Format term details into RDF/Turtle syntax for ontology inclusion.

    Args:
        term_details (dict): Dictionary with metadata for the new term.

    Returns:
        str: A string representing RDF Turtle triples to be appended.
    """
    term = f"\n# Added on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    term += f"mds:{term_details['term_name']} a owl:Class ;\n"
    term += f"    rdfs:label \"{term_details['label']}\" ;\n"
    term += f"    rdfs:subClassOf {term_details['parent_class']} ;\n"
    term += f"    skos:definition \"{term_details['definition']}\""
    
    if term_details['value_type'] != "None":
        term += " ;\n    mds:value " + term_details['value_type']
    
    if term_details['unit'] and term_details['unit'] != "None":
        term += " ;\n    mds:unit " + term_details['unit']
    
    term += " .\n"
    return term

def add_term_to_ontology(ontology_path):
    """
    Add a new term to a TTL ontology file and write the result to a new file.

    Args:
        ontology_path (str): Path to the existing ontology TTL file.

    Returns:
        bool: True if term was added successfully, False otherwise.
    """
    try:
        term_details = get_term_details()
        new_term = format_term(term_details)
        
        directory = os.path.dirname(ontology_path)
        filename = os.path.basename(ontology_path)
        name, ext = os.path.splitext(filename)
        new_filename = f"{name}_rxl895{ext}"
        new_path = os.path.join(directory, new_filename)
        
        with open(ontology_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        with open(new_path, 'w', encoding='utf-8') as f:
            f.write(content)
            f.write(new_term)
        
        print(f"\n✅ Successfully added new term 'mds:{term_details['term_name']}' to {new_path}")
        print(f"📁 Original file '{ontology_path}' was not modified.")
        return True

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return False

def main():
    """
    Main function to prompt the user for an ontology file and add a new term.
    """
    ontology_path = input("Enter the path to your ontology file: ").strip()
    
    if not os.path.exists(ontology_path):
        print(f"❌ Error: File '{ontology_path}' does not exist.")
        return
    
    add_term_to_ontology(ontology_path)

if __name__ == "__main__":
    main()
