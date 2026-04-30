import os
import json
import re
from pyld import jsonld
import copy
import random
import string
import warnings
from datetime import datetime
import uuid
import pandas as pd
import numpy as np
from rdflib import Graph, URIRef, Literal, Namespace, XSD
from rdflib.namespace import RDF, OWL, RDFS, DCTERMS
from urllib.parse import quote, urlparse
from ..InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from .. import helper_data as helper_data
import traceback
import hashlib
import requests
from importlib import resources
from .MDS_DF.main import MatDatSciDf

def load_licenses():
    with resources.files(helper_data).joinpath("licenseinfo.json").open() as f:
        spdx_data = json.load(f)
    return spdx_data

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


def extract_data_from_csv(
    metadata_template,
    csv_file,
    orcid,
    output_folder,
    row_key_cols=None,            # optional
    id_cols=None,                 # optional
    prop_column_pair_dict=None,   # optional
    ontology_graph=None,          # optional
    base_uri="https://cwrusdle.bitbucket.io/mds/",
    license=None #optional
):
    #raise Exception("called exeception")

    """
    Converts CSV rows into RDF graphs using a JSON-LD template and optional property mapping,
    writing JSON-LD files. This function assumes that the two rows below the header row contains the unit and the proper
    ontology name.

    Parameters
    ----------
    metadata_template : dict
        JSON-LD template with "@context" and "@graph".

    csv_file : str
        Path to the input CSV.

    row_key_cols : list[str]
        Columns to uniquely identify each row.

    id_cols : list[str]
        Columns that contain unique entity identifier independent of row.

    orcid : str
        ORCID identifier (dashes removed automatically).

    output_folder : str
        Directory to save JSON-LD files.

    prop_column_pair_dict : dict or None, optional
        Maps property keys to (subject_column, object_column) column pairs.
        If None or empty, no properties are added.

    ontology_graph : RDFLib Graph object or None, optional
        Ontology for property type/URI resolution.
        Required if prop_column_pair_dict is provided.

    base_uri : str, optional
        Base URI used to construct subject and object URIs.

    license : str, optional
        License to be used for the dataset.

    Returns
    -------
    List[rdflib.Graph]
        List of RDFLib Graphs, one per row.
    """

    df = pd.read_csv(csv_file)
    mds_df = MatDatSciDf(
                df = df,
                metadata_template=metadata_template,
                orcid=orcid,
                data_relations_dict=prop_column_pair_dict,
                metadata_rows=True,
                ontology_graph=ontology_graph,
                base_uri=base_uri
                )

    return mds_df.serialize_row(
            output_folder=output_folder,
            row_key_cols=row_key_cols,
            id_cols=id_cols,
            license=license,
            write_files=True
            )





def generate_prop_metadata_dict(ontology_graph):
    """
    Generates a dictionary where the keys are human-readable labels of object/datatype properties, and the values are
    2-tuples that contain the URI of that property in the first entry and the type (object/datatype) in second entry.

    Parameters
    ----------
    ontology_graph : RDFLib graph object of the ontology
        Path to the RDF/OWL ontology file.

    Returns
    -------
    dict
        Dictionary of the form:
        {
            "has material": ("http://example.org/ontology#hasMaterial", "Object Property"),
            "has value": ("http://example.org/ontology#hasValue", "Datatype Property"),
            ...
        }
    """

    prop_metadata_dict = {}

    for prop_type, label_type in [(OWL.ObjectProperty, "Object Property"), (OWL.DatatypeProperty, "Datatype Property")]:
        for prop in ontology_graph.subjects(RDF.type, prop_type):
            label = ontology_graph.value(prop, RDFS.label)
            if label:
                prop_metadata_dict[str(label)] = (str(prop), label_type)

    return prop_metadata_dict

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


def extract_from_folder(
    csv_folder, 
    metadata_template,
    orcid,  
    row_key_cols, 
    id_cols,
    output_base_folder, 
    prop_column_pair_dict=None, 
    ontology_graph=None,
    base_uri="https://cwrusdle.bitbucket.io/mds/",
    license=None
    ):
    """
    Processes all CSV files in a folder and converts each into RDF/JSON-LD files
    using a metadata template and optional object/datatype property mappings.

    Parameters
    ----------
    csv_folder : str
        Path to the folder containing CSV files.

    metadata_template : dict
        JSON-LD metadata template with "@context" and "@graph" describing the RDF structure.

    row_key_cols : list[str]
        List of CSV column names used to construct a unique key for each row.

    id_cols : list[str]
        Columns that contain unique entity identifier independent of row.

    orcid : str
        ORCID iD of the user (dashes will be removed automatically).

    output_base_folder : str
        Directory where output subfolders (one per CSV) will be created for JSON-LD files.

    prop_column_pair_dict : dict or None, optional
        Mapping from property key (e.g., predicate label) to list of (subject_column, object_column) tuples.
        These define additional object or datatype properties to inject based on CSV columns.
        If None, no extra connections are added.

    ontology_graph : str or None, optional
        RDFLib graph object of ontology from which property URIs and types are resolved.
        Required only if `prop_column_pair_dict` is given.

    base_uri : str, optional
        Base URI used to construct RDF subject and object URIs. Defaults to the CWRU MDS base.

    Returns
    -------
    None
        Writes JSON-LD files to disk. No return value.
    """

    os.makedirs(output_base_folder, exist_ok=True)
    # orcid = orcid.replace("-", "")

    if (license):
        write_license_triple(output_base_folder, base_uri, license)

    for filename in os.listdir(csv_folder):
        if not filename.endswith(".csv"):
            continue

        csv_path = os.path.join(csv_folder, filename)

        if row_key_cols:
            types_used = [
                entry["@type"].split(":")[-1]
                for entry in metadata_template.get("@graph", [])
                if "@type" in entry and entry.get("skos:altLabel") in row_key_cols
            ]
        else:
            types_used = []

        type_suffix = "-".join(set(types_used)) or "Unknown"
        uid = str(uuid.uuid4())[:8]
        folder_name = f"Dataset-{uid}-{type_suffix}"
        output_folder = os.path.join(output_base_folder, folder_name)

        os.makedirs(output_folder, exist_ok=True)
        
        extract_data_from_csv(
            metadata_template=metadata_template, 
            csv_file=csv_path,
            orcid=orcid,
            row_key_cols=row_key_cols, 
            id_cols=id_cols, 
            output_folder=output_folder, 
            prop_column_pair_dict=prop_column_pair_dict, 
            ontology_graph=ontology_graph, 
            base_uri=base_uri,
            license=license)


def extract_data_from_csv_interface(args):
    """
    CLI wrapper for extract_data_from_csv.
    Loads JSON/CSV/ontology files and calls the core function.
    """
    # Ensure output folder exists
    os.makedirs(args.output_folder, exist_ok=True)

    # Load metadata template
    with open(args.metadata_template, "r") as f:
        metadata_template = json.load(f)

    # Load ontology if given
    ontology_graph = None
    if args.ontology_path == "default" or args.ontology_path == None:
        ontology_graph = load_mds_ontology_graph()
    else:
        ontology_graph = Graph()
        ontology_graph.parse(args.ontology_path)

    # Call the core function
    return extract_data_from_csv(
        metadata_template=metadata_template,
        csv_file=args.csv_file,
        orcid=args.orcid,
        row_key_cols=args.row_key_cols,
        id_cols = args.id_cols,
        output_folder=args.output_folder,
        prop_column_pair_dict=args.prop_col,
        ontology_graph=ontology_graph,
        base_uri=args.base_uri,
        license=args.license
    )



