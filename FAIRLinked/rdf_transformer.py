import os
import hashlib
import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, XSD, DCTERMS
from datetime import datetime
import re
import traceback
import random
import string

try:
    from rdflib.namespace import QB
except ImportError:
    # If QB is not available in your environment:
    QB = Namespace('http://purl.org/linked-data/cube#')

from rdflib.namespace import SKOS
from FAIRLinked.input_handler import get_approved_id_columns, get_identifiers

# =============================================================================
#                            CONSTANTS
# =============================================================================

EXPERIMENT_ID_COLUMN = 'ExperimentId'   # recognized as a dimension if present
IS_MEASURE_FIELD = 'IsMeasure'
UNIT_FIELD = 'Unit'
CATEGORY_FIELD = 'Category'
EXISTING_URI_FIELD = 'ExistingURI'

DEFAULT_USER_PREFIX = 'mds'
DEFAULT_DATASET_NAME = 'SampleDataset'

ERROR_MSG_VARIABLE_NOT_FOUND = "Warning: Variable '{var_name}' not found in variable metadata. Skipping."
ERROR_MSG_PREFIX_NOT_FOUND = "Prefix '{prefix}' for unit '{unit_str}' not found in namespace map."

# =============================================================================
#                      FOLDER & FILE SETUP
# =============================================================================

def create_subfolders(root_folder_path: str) -> dict:
    """
    Description:
        Creates three subfolders ('ttl', 'jsonld', 'hash') under the specified root folder
        to store Turtle files (.ttl), JSON-LD files (.jsonld), and the SHA-256 hash files (.sha256).

    Algorithm:
        1) Define a list of subfolder names: ['ttl', 'jsonld', 'hash'].
        2) For each subfolder name:
           - Construct a path using os.path.join(root_folder_path, folder_name).
           - Create the directory if it doesn't exist (os.makedirs with exist_ok=True).
           - Store the result in a dictionary under the same key.
        3) Return this dictionary of paths.

    Args:
        root_folder_path (str): The path to the parent output folder where subfolders will be created.

    Returns:
        dict:
            A dictionary of subfolder paths, keyed by 'ttl', 'jsonld', and 'hash'.
    """
    subfolders = {}
    for folder_name in ["ttl", "jsonld", "hash"]:
        path = os.path.join(root_folder_path, folder_name)
        os.makedirs(path, exist_ok=True)
        subfolders[folder_name] = path
    return subfolders


def compute_file_hash(file_path: str) -> str:
    """
    Description:
        Computes the SHA-256 hash of a given file by reading its contents in chunks.

    Algorithm:
        1) Initialize a sha256 object (hashlib.sha256).
        2) Open the file in 'rb' mode.
        3) Read the file in 4096-byte chunks, updating the hash object each time.
        4) Return the hex digest of the final hash.

    Args:
        file_path (str):
            The path to the file that needs hashing.

    Returns:
        str:
            The SHA-256 hex digest string for the file contents.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as file:
        for byte_block in iter(lambda: file.read(4096), b''):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def create_root_folder(output_folder_path: str, dataset_name: str, orcid: str) -> tuple:
    """
    Description:
        Creates a top-level folder named 'Output_{orcidDigits}_{timestamp}' to store all
        outputs for the conversion process. Also prepares standardized strings for dataset
        and orcid usage throughout the process.

    Algorithm:
        1) Generate a timestamp in 'YYYYmmddHHMMSS' format.
        2) Extract digits from the user's ORCID => 'sanitized_orcid'.
        3) Sanitize 'dataset_name' => 'sanitized_dataset_name' by replacing non-word chars.
        4) Construct folder name => "Output_{sanitized_orcid}_{timestamp}".
        5) os.makedirs(...) to create the folder if not existing.
        6) Return (root_folder_path, overall_timestamp, sanitized_dataset_name, sanitized_orcid).

    Args:
        output_folder_path (str): The parent path where we create this top-level output folder.
        dataset_name (str): The dataset name to be sanitized (though not used in folder name).
        orcid (str): The user's ORCID, from which we extract digits.

    Returns:
        tuple:
            (
                root_folder_path (str): The newly created folder path,
                overall_timestamp (str): The run-specific timestamp (YYYYmmddHHMMSS),
                sanitized_dataset_name (str): The sanitized dataset name,
                sanitized_orcid (str): The numeric portion extracted from the ORCID.
            )
    """
    overall_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    sanitized_dataset_name = re.sub(r'\W|^(?=\d)', '_', dataset_name)
    sanitized_orcid = ''.join(re.findall(r'\d+', orcid))

    folder_name = f"Output_{sanitized_orcid}_{overall_timestamp}"
    root_folder_path = os.path.join(output_folder_path, folder_name)
    os.makedirs(root_folder_path, exist_ok=True)

    return root_folder_path, overall_timestamp, sanitized_dataset_name, sanitized_orcid


# =============================================================================
#      WRITING A .TXT FILE DESCRIBING NAMING CONVENTIONS
# =============================================================================

def write_naming_conventions_doc(root_folder_path: str,
                                 conversion_mode: str,
                                 orcid: str,
                                 overall_timestamp: str,
                                 dataset_name: str) -> None:
    """
    Description:
        Writes a text file (naming_conventions_{orcidDigits}_{timestamp}.txt) describing
        how the DataSet, Slice, SliceKey, and filenames are named in the chosen mode
        ('entire' or 'row-by-row').

    Algorithm:
        1) Extract numeric digits from 'orcid' => numeric_orcid.
        2) Build the .txt filename => naming_conventions_{numeric_orcid}_{overall_timestamp}.txt.
        3) Depending on 'conversion_mode', build a descriptive message about naming patterns.
        4) Write this message to the .txt file in 'root_folder_path'.

    Args:
        root_folder_path (str): The folder where we store the .txt file.
        conversion_mode (str): 'entire' or 'row-by-row' mode.
        orcid (str): The user's ORCID (for numeric extraction).
        overall_timestamp (str): The run-specific timestamp used for naming outputs.
        dataset_name (str): The sanitized dataset name to reference in the doc.

    Returns:
        None
    """
    numeric_orcid = ''.join(re.findall(r'\d+', orcid))
    txt_filename = f"naming_conventions_{numeric_orcid}_{overall_timestamp}.txt"
    txt_path = os.path.join(root_folder_path, txt_filename)

    if conversion_mode == "entire":
        # Single qb:DataSet with multiple qb:Slices
        msg = (
            "Naming Conventions for ENTIRE Dataset Mode\n"
            "-----------------------------------------\n"
            f"Root folder: Output_{numeric_orcid}_{overall_timestamp}\n\n"
            "We produce subfolders 'ttl', 'jsonld', 'hash'.\n\n"
            f"Single qb:DataSet => mds:Dataset_{dataset_name}_{numeric_orcid}_{overall_timestamp}\n"
            f"Single qb:SliceKey => mds:SliceKey_{dataset_name}_{numeric_orcid}_{overall_timestamp}\n"
            "Each row => qb:Slice => mds:Slice_{anyApprovedIDs}_{orcidDigits}_{timestamp}\n\n"
            "Output filenames typically:\n"
            f"  {dataset_name}_{numeric_orcid}_{overall_timestamp}.ttl\n"
            f"  {dataset_name}_{numeric_orcid}_{overall_timestamp}.jsonld\n"
            f"  {dataset_name}_{numeric_orcid}_{overall_timestamp}.jsonld.sha256\n\n"
            "Where {anyApprovedIDs} are user-approved ID columns (e.g. 'ExperimentId').\n"
        )
    elif conversion_mode == "row-by-row":
        # row-by-row mode => each row => separate DataSet
        msg = (
            "Naming Conventions for ROW-BY-ROW Mode\n"
            "--------------------------------------\n"
            f"Root folder: Output_{numeric_orcid}_{overall_timestamp}\n\n"
            "We produce subfolders 'ttl', 'jsonld', 'hash'.\n\n"
            "Each row => separate qb:DataSet => mds:Dataset_{anyApprovedIDs}_{orcidDigits}_{timestamp}\n"
            "           plus a qb:Slice => mds:Slice_{anyApprovedIDs}_{orcidDigits}_{timestamp}\n"
            "           and a qb:SliceKey => mds:SliceKey_{anyApprovedIDs}_{orcidDigits}_{timestamp}\n\n"
            "Output filenames per row:\n"
            "   {anyApprovedIDs}_{orcidDigits}_{timestamp}.ttl\n"
            "   {anyApprovedIDs}_{orcidDigits}_{timestamp}.jsonld\n"
            "   {anyApprovedIDs}_{orcidDigits}_{timestamp}.jsonld.sha256\n\n"
            "Where {anyApprovedIDs} are user-approved ID columns, e.g. 'ExperimentId'.\n"
        )
    else:
        msg = (
            "Naming Conventions for CRADLE Mode\n"
            "--------------------------------------\n"
            f"Root folder: Output_{numeric_orcid}_{overall_timestamp}\n\n"
            "We produce subfolders 'ttl', 'jsonld', 'hash'.\n\n"
            "Each row => separate qb:DataSet => mds:Dataset_{randomLetter}_{enteredIDs}_{anyApprovedIDs}_{orcidDigits}_{timestamp}\n"
            "           plus a qb:Slice => mds:Slice_{randomLetter}_{enteredIDs}_{anyApprovedIDs}_{orcidDigits}_{timestamp}\n"
            "           and a qb:SliceKey => mds:SliceKey_{randomLetter}_{enteredIDs}_{anyApprovedIDs}_{orcidDigits}_{timestamp}\n\n"
            "Output filenames per row:\n"
            "   {randomLetter}_{enteredIDs}_{anyApprovedIDs}_{orcidDigits}_{timestamp}.ttl\n"
            "   {randomLetter}_{enteredIDs}_{anyApprovedIDs}_{orcidDigits}_{timestamp}.jsonld\n"
            "   {randomLetter}_{enteredIDs}_{anyApprovedIDs}_{orcidDigits}_{timestamp}.jsonld.sha256\n\n"
            "Where {enteredIDs} are additional top level concept IDs and {anyApprovedIDs} are user-approved ID columns, e.g. 'ExperimentId'.\n"
        )

    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(msg)


# =============================================================================
#        HELPER: NAMESPACE PREPARATION
# =============================================================================

def prepare_namespaces(namespace_map: dict, user_chosen_prefix: str = DEFAULT_USER_PREFIX) -> dict:
    """
    Description:
        Validates the user-defined prefix in namespace_map, ensures each URI ends with
        '/' or '#', and converts them to rdflib.Namespace objects.

    Algorithm:
        1) Copy the input namespace_map.
        2) Check if user_chosen_prefix in the map; raise an error if missing.
        3) For each (prefix, uri):
           - Validate it starts with http(s).
           - Ensure it ends with '/' or '#'.
        4) Convert each to Namespace(...).
        5) Return the new dictionary.

    Args:
        namespace_map (dict):
            Keys => prefix (str), Values => base URI (str).
        user_chosen_prefix (str):
            The prefix the user wants to use for new IRIs (default: 'mds').

    Returns:
        dict:
            A mapping of prefix => rdflib.Namespace objects.

    Raises:
        ValueError if user_chosen_prefix is missing or if any URI is invalid.
    """
    ns_map = namespace_map.copy()
    if user_chosen_prefix not in ns_map:
        raise ValueError(f"Namespace URI for prefix '{user_chosen_prefix}' required.")

    for prefix, uri in ns_map.items():
        if not re.match(r'^https?://', uri):
            raise ValueError(f"Invalid URI '{uri}' for prefix '{prefix}'.")
        if not uri.endswith(('/', '#')):
            ns_map[prefix] = uri + '#'
    return {prefix: Namespace(uri) for prefix, uri in ns_map.items()}


# =============================================================================
#   SANITIZERS (IRIs vs Filenames)
# =============================================================================

def _sanitize_for_iri(s: str) -> str:
    """
    Description:
        Cleans up a string for use as an IRI local name by:
         - replacing all non-word chars with '_'
         - if it begins with a digit, prepend '_'
         - condensing multiple underscores to one
         - removing trailing underscores

    Algorithm:
        1) re.sub non-word => '_'
        2) if it starts with a digit => prepend '_'
        3) re.sub multiple => single underscore
        4) strip trailing underscores

    Args:
        s (str): The raw string needing sanitization.

    Returns:
        str: A cleaned version suitable for IRIs, e.g. "Detector_1".
    """
    cleaned = re.sub(r'[^\w]+', '_', s)
    if re.match(r'^\d', cleaned):
        cleaned = f"_{cleaned}"
    cleaned = re.sub(r'__+', '_', cleaned)
    cleaned = cleaned.strip('_')
    return cleaned


def _sanitize_for_filename(s: str) -> str:
    """
    Description:
        Cleans up a string for filenames by replacing non-word chars => '_' and not forcibly
        prepending underscores if it starts with a digit. Also condenses multiple underscores
        and strips trailing underscores.

    Algorithm:
        1) re.sub non-word => '_'
        2) re.sub multiple => single underscore
        3) strip trailing underscores

    Args:
        s (str): The raw string.

    Returns:
        str: A cleaned version more suitable for filenames, e.g. "Detector_1" or "3_14".
    """
    cleaned = re.sub(r'[^\w]+', '_', s)
    cleaned = re.sub(r'__+', '_', cleaned)
    cleaned = cleaned.strip('_')
    return cleaned


# =============================================================================
#  UTILS
# =============================================================================

def get_property_uri(var_name: str, meta: dict, ns_map: dict, user_ns: Namespace) -> URIRef:
    """
    Description:
        Obtains the appropriate URIRef for a given variable. If 'ExistingURI' is provided,
        attempts to parse it as 'prefix:LocalPart' or a full URI. Otherwise, uses the user
        namespace + sanitized var_name.

    Algorithm:
        1) If meta has 'ExistingURI':
           a) if it has ':' => split prefix vs. local_part
              => look up in ns_map => create ns[local_part]
           b) else treat it as a full URIRef
        2) Otherwise => user_ns[var_name], with spaces replaced by underscores.

    Args:
        var_name (str): The DataFrame column name.
        meta (dict): The metadata for that variable (keys e.g. 'ExistingURI').
        ns_map (dict): A dictionary mapping prefix => rdflib.Namespace.
        user_ns (Namespace): The user-chosen prefix's Namespace object.

    Returns:
        rdflib.term.URIRef: The final property URI for this variable.

    Raises:
        ValueError if prefix not found in ns_map.
    """
    existing_uri = meta.get(EXISTING_URI_FIELD)
    if existing_uri:
        if ':' in existing_uri:
            prefix, local_part = existing_uri.split(':', 1)
            ns = ns_map.get(prefix)
            if not ns:
                raise ValueError(f"Prefix '{prefix}' not found for ExistingURI '{existing_uri}'")
            return ns[local_part]
        else:
            # treat as a full URI
            return URIRef(existing_uri)
    else:
        sanitized_var_name = var_name.replace(' ', '_')
        return user_ns[sanitized_var_name]


def extract_variables(variable_metadata: dict, df_columns: list) -> tuple:
    """
    Description:
        Splits columns into 'dimensions' or 'measures' by checking variable_metadata[var_name]['IsMeasure'].

    Algorithm:
        1) Initialize dimensions = [] and measures = [].
        2) For each column in df_columns:
           - Retrieve meta = variable_metadata.get(column).
           - If meta is found, check 'IsMeasure' => 'yes'? => measures, else => dimensions.
           - If not found, log a warning message.
        3) Return (dimensions, measures).

    Args:
        variable_metadata (dict): A dict mapping column => metadata (with 'IsMeasure', etc.).
        df_columns (list): The list of column names from the DataFrame.

    Returns:
        (list, list):
            A tuple of (dimensions, measures).
    """
    dimensions = []
    measures = []
    for var_name in df_columns:
        meta = variable_metadata.get(var_name)
        if meta:
            is_measure_value = meta.get(IS_MEASURE_FIELD)
            is_measure = (
                isinstance(is_measure_value, str) and 
                is_measure_value.strip().lower() == 'yes'
            )
            if is_measure:
                measures.append(var_name)
            else:
                dimensions.append(var_name)
        else:
            print(ERROR_MSG_VARIABLE_NOT_FOUND.format(var_name=var_name))
    return dimensions, measures


def process_unit(unit_str: str, ns_map: dict, user_ns: Namespace) -> URIRef:
    """
    Description:
        Interprets a 'Unit' string from variable_metadata and returns an rdflib.URIRef
        for that unit. If it's 'prefix:LocalPart', we look up prefix in ns_map. Otherwise,
        we treat it as user_ns[unitStr] (with spaces replaced).

    Algorithm:
        1) If unit_str is empty => return None.
        2) If ':' in unit_str => parse prefix, local_part => ns_map[prefix][local_part].
        3) Otherwise => user_ns[unit_str], replacing spaces with underscores.
        4) If prefix not found => raise ValueError.

    Args:
        unit_str (str): e.g. "qudt:MilliM" or "MyLocalUnit".
        ns_map (dict): prefix => Namespace
        user_ns (Namespace): the user prefix's namespace.

    Returns:
        rdflib.URIRef or None if no unit_str given.

    Raises:
        ValueError if prefix is missing from ns_map.
    """
    if not unit_str:
        return None
    try:
        if ':' in unit_str:
            prefix, local_part = unit_str.split(':', 1)
            unit_ns = ns_map.get(prefix)
            if not unit_ns:
                raise ValueError(ERROR_MSG_PREFIX_NOT_FOUND.format(prefix=prefix, unit_str=unit_str))
            return unit_ns[local_part]
        else:
            return user_ns[unit_str.replace(' ', '_')]
    except Exception as e:
        print(f"Error processing unit '{unit_str}': {e}")
        return None
    
def _create_id_string(id_dict) -> str:
    """
    Function to turn user-entered IDs into a string for CRADLE naming
    """
    id_string = ""
    for key in id_dict:
        if id_dict[key] != "":
            id_string += "_" + str(id_dict[key])

    return id_string


# =============================================================================
#        BUILDING THE RDF DATA CUBE: DSD & Observations
# =============================================================================

def add_component_to_dsd(dsd_graph: Graph,
                         dsd_uri: URIRef,
                         prop_uri: URIRef,
                         component_type: URIRef,
                         prop_type: URIRef) -> None:
    """
    Description:
        Adds a dimension/measure/attribute property to the qb:DataStructureDefinition
        by creating a blank node and linking it accordingly.

    Algorithm:
        1) Create a blank node => component.
        2) dsd_uri -- qb:component --> component
        3) component -- (component_type) --> prop_uri  (e.g. dimension => prop_uri)
        4) prop_uri -- rdf:type --> prop_type

    Args:
        dsd_graph (Graph): The Graph that holds the DSD.
        dsd_uri (URIRef): The DataStructureDefinition node.
        prop_uri (URIRef): The property URI for this dimension/measure/attribute.
        component_type (URIRef): e.g. qb:dimension, qb:measure, qb:attribute.
        prop_type (URIRef): e.g. qb:DimensionProperty, qb:MeasureProperty, qb:AttributeProperty.

    Returns:
        None
    """
    component = BNode()
    dsd_graph.add((dsd_uri, QB.component, component))
    dsd_graph.add((component, component_type, prop_uri))
    dsd_graph.add((prop_uri, RDF.type, prop_type))


def create_dsd(variable_metadata: dict,
               dimensions: list,
               measures: list,
               ns_map: dict,
               user_ns: Namespace) -> tuple:
    """
    Description:
        Builds a qb:DataStructureDefinition for the given dimensions and measures.
        Also sets up measureType as a dimension. Adds optional qb:attribute for 'unitMeasure'
        and 'category' if present.

    Algorithm:
        1) Initialize an empty Graph, bind namespace prefixes.
        2) Create dsd_uri = user_ns["DataStructureDefinition"], mark it as qb:DataStructureDefinition.
        3) For each dimension => add_component_to_dsd(...).
        4) Add measureType as dimension.
        5) For each measure => add_component_to_dsd(...).
        6) If any columns require 'unitMeasure' or 'category', add qb:attribute.
        7) Return (dsd_graph, dsd_uri).

    Args:
        variable_metadata (dict): Maps column => metadata (like AltLabel, Category, etc.).
        dimensions (list): The dimension column names.
        measures (list): The measure column names.
        ns_map (dict): prefix => Namespace
        user_ns (Namespace): The user-chosen prefix's Namespace.

    Returns:
        (Graph, URIRef):
            dsd_graph: The Graph containing the DSD definitions.
            dsd_uri: The URIRef for the qb:DataStructureDefinition.
    """
    dsd_graph = Graph()
    for prefix, ns_obj in ns_map.items():
        dsd_graph.bind(prefix, ns_obj)
    dsd_graph.bind('skos', SKOS)

    dsd_uri = user_ns["DataStructureDefinition"]
    dsd_graph.add((dsd_uri, RDF.type, QB.DataStructureDefinition))

    # 1) Dimensions (excluding measureType)
    for var_name in dimensions:
        meta = variable_metadata[var_name]
        dim_prop = get_property_uri(var_name, meta, ns_map, user_ns)
        add_component_to_dsd(dsd_graph, dsd_uri, dim_prop, QB.dimension, QB.DimensionProperty)

        alt_label = meta.get("AltLabel")
        if alt_label:
            dsd_graph.add((dim_prop, SKOS.altLabel, Literal(alt_label)))
        if CATEGORY_FIELD in meta and meta[CATEGORY_FIELD]:
            cat_prop = user_ns['category']
            cat_uri = user_ns[meta[CATEGORY_FIELD].replace(' ', '_')]
            dsd_graph.add((dim_prop, cat_prop, cat_uri))

    # measureType dimension
    add_component_to_dsd(dsd_graph, dsd_uri, QB.measureType, QB.dimension, QB.DimensionProperty)

    # 2) Measures
    for var_name in measures:
        meta = variable_metadata[var_name]
        meas_prop = get_property_uri(var_name, meta, ns_map, user_ns)
        add_component_to_dsd(dsd_graph, dsd_uri, meas_prop, QB.measure, QB.MeasureProperty)

        alt_label = meta.get("AltLabel")
        if alt_label:
            dsd_graph.add((meas_prop, SKOS.altLabel, Literal(alt_label)))
        if CATEGORY_FIELD in meta and meta[CATEGORY_FIELD]:
            cat_prop = user_ns['category']
            cat_uri = user_ns[meta[CATEGORY_FIELD].replace(' ', '_')]
            dsd_graph.add((meas_prop, cat_prop, cat_uri))

    # 3) Check if we need qb:attribute for 'unitMeasure' or 'category'
    attributes_used = set()
    for var_name, meta in variable_metadata.items():
        if UNIT_FIELD in meta and meta[UNIT_FIELD]:
            attributes_used.add('unitMeasure')
        if CATEGORY_FIELD in meta and meta[CATEGORY_FIELD]:
            attributes_used.add('category')

    sdmx_attr = ns_map.get('sdmx-attribute')
    if 'unitMeasure' in attributes_used and sdmx_attr:
        unit_meas = sdmx_attr['unitMeasure']
        add_component_to_dsd(dsd_graph, dsd_uri, unit_meas, QB.attribute, QB.AttributeProperty)
    elif 'unitMeasure' in attributes_used and not sdmx_attr:
        print("Warning: 'sdmx-attribute' not found. Skipping unitMeasure attribute.")

    if 'category' in attributes_used:
        cat_prop = user_ns['category']
        add_component_to_dsd(dsd_graph, dsd_uri, cat_prop, QB.attribute, QB.AttributeProperty)

    return dsd_graph, dsd_uri


def create_observation(dataset_graph: Graph,
                       row: pd.Series,
                       variable_metadata: dict,
                       variable_dimensions: list,
                       measures: list,
                       ns_map: dict,
                       user_ns: Namespace,
                       observation_counter: int) -> tuple:
    """
    Description:
        For each measure in 'measures', if the row has a non-null value, create a qb:Observation
        node. Link measureType => measure property, store measure value, dimension values (from
        variable_dimensions), and optionally link the unit measure if found.

    Algorithm:
        1) For each measure in 'measures':
           a) If row[measure] is not null => create observation_{observation_counter} as qb:Observation.
           b) Add triple (obs_uri, qb:measureType, measure_prop).
           c) Add triple (obs_uri, measure_prop, measure_value).
           d) For each dimension in variable_dimensions => store dimension_value or NotFound.
           e) If UNIT_FIELD => link sdmx-attribute:unitMeasure => unit URI.
        2) Return (list_of_observation_uris, updated_observation_counter).

    Args:
        dataset_graph (Graph): The graph where we store Observations and data.
        row (pd.Series): A single row from the DataFrame.
        variable_metadata (dict): column => metadata.
        variable_dimensions (list): the subset of dimensions that vary in this context.
        measures (list): measure column names.
        ns_map (dict): prefix => Namespace.
        user_ns (Namespace): the user-chosen prefix's Namespace object.
        observation_counter (int): the current global counter for numbering Observations.

    Returns:
        (list_of_obs_uris, updated_counter):
            A list of the newly created observation URIs, plus the incremented observation_counter.
    """
    sdmx_attr = ns_map.get('sdmx-attribute')
    not_found_uri = user_ns['NotFound']

    observations = []
    for measure_name in measures:
        measure_value = row.get(measure_name)
        if pd.notnull(measure_value):
            meta = variable_metadata[measure_name]
            measure_prop = get_property_uri(measure_name, meta, ns_map, user_ns)
            obs_uri = user_ns[f"observation_{observation_counter}"]
            observation_counter += 1

            dataset_graph.add((obs_uri, RDF.type, QB.Observation))
            dataset_graph.add((obs_uri, QB.measureType, measure_prop))

            # measure value
            try:
                val_literal = Literal(float(measure_value), datatype=XSD.double)
            except (ValueError, TypeError):
                val_literal = Literal(measure_value)
            dataset_graph.add((obs_uri, measure_prop, val_literal))

            # dimension values
            for dim_name in variable_dimensions:
                dim_val = row.get(dim_name)
                dim_meta = variable_metadata[dim_name]
                dim_prop = get_property_uri(dim_name, dim_meta, ns_map, user_ns)
                if pd.notnull(dim_val):
                    dataset_graph.add((obs_uri, dim_prop, Literal(dim_val)))
                else:
                    dataset_graph.add((obs_uri, dim_prop, not_found_uri))

            # unit measure
            unit_uri = process_unit(meta.get(UNIT_FIELD), ns_map, user_ns)
            if unit_uri and sdmx_attr:
                dataset_graph.add((obs_uri, sdmx_attr['unitMeasure'], unit_uri))
            elif unit_uri and not sdmx_attr:
                print("Warning: 'sdmx-attribute' missing. Skipping unitMeasure attribute.")

            observations.append(obs_uri)

    return observations, observation_counter


# =============================================================================
#          ROW-BY-ROW CONVERSION
# =============================================================================

def convert_row_by_row(
    df: pd.DataFrame,
    variable_metadata: dict,
    ns_map: dict,
    user_chosen_prefix: str,
    orcid: str,
    root_folder_path: str,
    overall_timestamp: str
):
    """
    Description:
        Converts each row of a DataFrame into its own qb:DataSet in RDF, prompting the user
        to choose which ID columns to incorporate in naming. Each row-based dataset shares
        the same folder timestamp to keep them grouped.

    Algorithm:
        1) Identify candidate ID columns (contain 'id'), pass 'row-by-row' to get_approved_id_columns(...).
        2) Extract which columns are dimensions vs. measures => create a single DSD for entire DF.
        3) For each row => build a new Graph that:
            - Copies the DSD
            - Creates a new qb:DataSet => mds:Dataset_{someIDs}_{orcid}_{timestamp}
            - Creates a SliceKey => mds:SliceKey_{someIDs}_{orcid}_{timestamp}
            - Creates a Slice => mds:Slice_{someIDs}_{orcid}_{timestamp}
            - Adds Observations for each measure
        4) Write each row's TTL/JSON-LD + .sha256 hash in subfolders.

    Args:
        df (pd.DataFrame): The entire DataFrame to convert row-by-row.
        variable_metadata (dict): column => metadata dictionary.
        ns_map (dict): prefix => Namespace mapping.
        user_chosen_prefix (str): e.g. 'mds'.
        orcid (str): The user's ORCID, from which we extract digits.
        root_folder_path (str): The top-level folder for outputs.
        overall_timestamp (str): The run's global timestamp for consistent naming.

    Returns:
        None
    """
    user_ns = ns_map[user_chosen_prefix]

    candidate_id_cols = [c for c in df.columns if re.search(r'id', c, re.IGNORECASE)]
    approved_id_cols = get_approved_id_columns(candidate_id_cols, mode='row-by-row')

    # Build a single DSD for entire DF
    dimensions, measures = extract_variables(variable_metadata, df.columns)
    if EXPERIMENT_ID_COLUMN in df.columns and EXPERIMENT_ID_COLUMN not in dimensions:
        dimensions.insert(0, EXPERIMENT_ID_COLUMN)

    dsd_graph, dsd_uri = create_dsd(variable_metadata, dimensions, measures, ns_map, user_ns)

    # For each row => new dataset
    for idx, row in df.iterrows():
        # naming: from approved ID columns
        name_parts_file = []
        name_parts_iri = []
        for col in approved_id_cols:
            val = row[col] if pd.notnull(row[col]) else "NotFound"
            name_parts_file.append(_sanitize_for_filename(str(val)))
            name_parts_iri.append(_sanitize_for_iri(str(val)))

        numeric_orcid = ''.join(re.findall(r'\d+', orcid))

        # fallback => orcid + timestamp
        if any(name_parts_file):
            combined_file = "_".join(name_parts_file + [numeric_orcid, overall_timestamp])
        else:
            combined_file = f"{numeric_orcid}_{overall_timestamp}"
        combined_file = _sanitize_for_filename(combined_file)

        if any(name_parts_iri):
            combined_iri = "_".join(name_parts_iri + [numeric_orcid, overall_timestamp])
        else:
            combined_iri = f"{numeric_orcid}_{overall_timestamp}"
        combined_iri = _sanitize_for_iri(combined_iri)

        dataset_id_str = _sanitize_for_iri(f"Dataset_{combined_iri}")
        slice_id_str = _sanitize_for_iri(f"Slice_{combined_iri}")
        slice_key_id = _sanitize_for_iri(f"SliceKey_{combined_iri}")

        dataset_uri = user_ns[dataset_id_str]
        slice_uri = user_ns[slice_id_str]
        slice_key_uri = user_ns[slice_key_id]

        # Copy the DSD graph into a fresh row_graph
        row_graph = dsd_graph.__class__()
        for prefix, ns_obj in ns_map.items():
            row_graph.bind(prefix, ns_obj)
        row_graph.bind('skos', SKOS)
        for triple in dsd_graph:
            row_graph.add(triple)

        # qb:DataSet
        row_graph.add((dataset_uri, RDF.type, QB.DataSet))
        row_graph.add((dataset_uri, QB.structure, dsd_uri))
        row_graph.add((dataset_uri, DCTERMS.title, Literal(dataset_id_str)))
        row_graph.add((dataset_uri, DCTERMS.creator, Literal(orcid)))

        # Single SliceKey for these dimensions
        row_graph.add((slice_key_uri, RDF.type, QB.SliceKey))
        fixed_dimensions = dimensions
        for dim_name in fixed_dimensions:
            dim_prop = get_property_uri(dim_name, variable_metadata[dim_name], ns_map, user_ns)
            row_graph.add((slice_key_uri, QB.componentProperty, dim_prop))

        # Single Slice
        row_graph.add((slice_uri, RDF.type, QB.Slice))
        row_graph.add((slice_uri, QB.sliceStructure, slice_key_uri))
        row_graph.add((dataset_uri, QB.slice, slice_uri))

        not_found_uri = user_ns['NotFound']
        for dim_name in fixed_dimensions:
            dim_val = row.get(dim_name)
            dim_meta = variable_metadata[dim_name]
            dim_prop = get_property_uri(dim_name, dim_meta, ns_map, user_ns)
            if pd.notnull(dim_val):
                row_graph.add((slice_uri, dim_prop, Literal(dim_val)))
            else:
                row_graph.add((slice_uri, dim_prop, not_found_uri))

        # Observations
        obs_counter = 1
        variable_dims = []
        observations, obs_counter = create_observation(
            row_graph, row, variable_metadata, variable_dims, measures, ns_map, user_ns, obs_counter
        )
        for obs_uri in observations:
            row_graph.add((slice_uri, QB.observation, obs_uri))

        # Write to subfolders
        subfolders = create_subfolders(root_folder_path)
        ttl_path = os.path.join(subfolders["ttl"], f"{combined_file}.ttl")
        jsonld_path = os.path.join(subfolders["jsonld"], f"{combined_file}.jsonld")
        hash_path = os.path.join(subfolders["hash"], f"{combined_file}.jsonld.sha256")

        row_graph.serialize(destination=ttl_path, format='turtle')
        row_graph.serialize(destination=jsonld_path, format='json-ld', auto_compact=True)

        # compute hash
        file_hash = compute_file_hash(jsonld_path)
        with open(hash_path, 'w') as hf:
            hf.write(file_hash)

# =============================================================================
#          ROW-BY-ROW FOR CRADLE
# =============================================================================

def convert_row_by_row_CRADLE(
    df: pd.DataFrame,
    variable_metadata: dict,
    ns_map: dict,
    user_chosen_prefix: str,
    orcid: str,
    root_folder_path: str,
    overall_timestamp: str
):
    """
    Description:
        Converts each row of a DataFrame into its own qb:DataSet in RDF, prompting the user
        to choose which ID columns to incorporate in naming. Each row-based dataset shares
        the same folder timestamp to keep them grouped.

    Algorithm:
        1) Identify candidate ID columns (contain 'id'), pass 'row-by-row' to get_approved_id_columns(...).
        2) Extract which columns are dimensions vs. measures => create a single DSD for entire DF.
        3) For each row => build a new Graph that:
            - Copies the DSD
            - Creates a new qb:DataSet => mds:Dataset_{someIDs}_{orcid}_{timestamp}
            - Creates a SliceKey => mds:SliceKey_{someIDs}_{orcid}_{timestamp}
            - Creates a Slice => mds:Slice_{someIDs}_{orcid}_{timestamp}
            - Adds Observations for each measure
        4) Write each row's TTL/JSON-LD + .sha256 hash in subfolders.

    Args:
        df (pd.DataFrame): The entire DataFrame to convert row-by-row.
        variable_metadata (dict): column => metadata dictionary.
        ns_map (dict): prefix => Namespace mapping.
        user_chosen_prefix (str): e.g. 'mds'.
        orcid (str): The user's ORCID, from which we extract digits.
        root_folder_path (str): The top-level folder for outputs.
        overall_timestamp (str): The run's global timestamp for consistent naming.

    Returns:
        None
    """
    user_ns = ns_map[user_chosen_prefix]

    candidate_id_cols = [c for c in df.columns if re.search(r'id', c, re.IGNORECASE)]
    approved_id_cols = get_approved_id_columns(candidate_id_cols, mode='row-by-row')

    missing_ids = get_identifiers(approved_id_cols)

    # Build a single DSD for entire DF
    dimensions, measures = extract_variables(variable_metadata, df.columns)
    if EXPERIMENT_ID_COLUMN in df.columns and EXPERIMENT_ID_COLUMN not in dimensions:
        dimensions.insert(0, EXPERIMENT_ID_COLUMN)

    dsd_graph, dsd_uri = create_dsd(variable_metadata, dimensions, measures, ns_map, user_ns)

    # For each row => new dataset
    for idx, row in df.iterrows():
        # naming: from approved ID columns
        name_parts_file = []
        name_parts_iri = []
        for col in approved_id_cols:
            val = row[col] if pd.notnull(row[col]) else "NotFound"
            name_parts_file.append(_sanitize_for_filename(str(val)))
            name_parts_iri.append(_sanitize_for_iri(str(val)))

        numeric_orcid = ''.join(re.findall(r'\d+', orcid))

        # fallback => orcid + timestamp
        if any(name_parts_file):
            combined_file = "_".join(name_parts_file + [numeric_orcid, overall_timestamp])
        else:
            combined_file = f"{numeric_orcid}_{overall_timestamp}"
        combined_file = _sanitize_for_filename(combined_file)
        letter = random.choice(string.ascii_lowercase)
        combined_file = letter + _create_id_string(missing_ids) + "_" + combined_file
        

        if any(name_parts_iri):
            combined_iri = "_".join(name_parts_iri + [numeric_orcid, overall_timestamp])
        else:
            combined_iri = f"{numeric_orcid}_{overall_timestamp}"
        combined_iri = _sanitize_for_iri(combined_iri)
        combined_iri = letter + _create_id_string(missing_ids) + "_" + combined_iri

        dataset_id_str = _sanitize_for_iri(f"Dataset_{combined_iri}")
        slice_id_str = _sanitize_for_iri(f"Slice_{combined_iri}")
        slice_key_id = _sanitize_for_iri(f"SliceKey_{combined_iri}")

        dataset_uri = user_ns[dataset_id_str]
        slice_uri = user_ns[slice_id_str]
        slice_key_uri = user_ns[slice_key_id]

        # Copy the DSD graph into a fresh row_graph
        row_graph = dsd_graph.__class__()
        for prefix, ns_obj in ns_map.items():
            row_graph.bind(prefix, ns_obj)
        row_graph.bind('skos', SKOS)
        for triple in dsd_graph:
            row_graph.add(triple)

        # qb:DataSet
        row_graph.add((dataset_uri, RDF.type, QB.DataSet))
        row_graph.add((dataset_uri, QB.structure, dsd_uri))
        row_graph.add((dataset_uri, DCTERMS.title, Literal(dataset_id_str)))
        row_graph.add((dataset_uri, DCTERMS.creator, Literal(orcid)))

        # Single SliceKey for these dimensions
        row_graph.add((slice_key_uri, RDF.type, QB.SliceKey))
        fixed_dimensions = dimensions
        for dim_name in fixed_dimensions:
            dim_prop = get_property_uri(dim_name, variable_metadata[dim_name], ns_map, user_ns)
            row_graph.add((slice_key_uri, QB.componentProperty, dim_prop))

        # Single Slice
        row_graph.add((slice_uri, RDF.type, QB.Slice))
        row_graph.add((slice_uri, QB.sliceStructure, slice_key_uri))
        row_graph.add((dataset_uri, QB.slice, slice_uri))

        not_found_uri = user_ns['NotFound']
        for dim_name in fixed_dimensions:
            dim_val = row.get(dim_name)
            dim_meta = variable_metadata[dim_name]
            dim_prop = get_property_uri(dim_name, dim_meta, ns_map, user_ns)
            if pd.notnull(dim_val):
                row_graph.add((slice_uri, dim_prop, Literal(dim_val)))
            else:
                row_graph.add((slice_uri, dim_prop, not_found_uri))

        # Observations
        obs_counter = 1
        variable_dims = []
        observations, obs_counter = create_observation(
            row_graph, row, variable_metadata, variable_dims, measures, ns_map, user_ns, obs_counter
        )
        for obs_uri in observations:
            row_graph.add((slice_uri, QB.observation, obs_uri))

        # Write to subfolders
        subfolders = create_subfolders(root_folder_path)
        ttl_path = os.path.join(subfolders["ttl"], f"{combined_file}.ttl")
        jsonld_path = os.path.join(subfolders["jsonld"], f"{combined_file}.jsonld")
        hash_path = os.path.join(subfolders["hash"], f"{combined_file}.jsonld.sha256")

        row_graph.serialize(destination=ttl_path, format='turtle')
        row_graph.serialize(destination=jsonld_path, format='json-ld', auto_compact=True)


        # compute hash
        file_hash = compute_file_hash(jsonld_path)
        with open(hash_path, 'w') as hf:
            hf.write(file_hash)


# =============================================================================
#          ENTIRE-DATASET CONVERSION
# =============================================================================

def convert_entire_dataset(
    df: pd.DataFrame,
    variable_metadata: dict,
    ns_map: dict,
    user_chosen_prefix: str,
    dataset_name: str,
    orcid: str,
    output_folder_paths: dict,
    fixed_dimensions: list = None,
    overall_timestamp: str = None
):
    """
    Description:
        Converts the entire DataFrame into a single qb:DataSet, and each row becomes
        a qb:Slice. Observations are created for each measure in each row. 
        If user picks ID columns => these become part of the slice's name.

    Algorithm:
        1) Identify columns with 'id'. Prompt user with mode='entire' => get_approved_id_columns(...).
        2) Create a DataStructureDefinition for the entire DF => dimensions + measures.
        3) Add a single qb:DataSet => e.g. mds:Dataset_{datasetName}.
        4) Create a single qb:SliceKey referencing dimension properties.
        5) For each row => create qb:Slice => name derived from (someIDs + orcid + timestamp).
        6) Within that slice => create Observations (one per measure).
        7) Write a single .ttl/.jsonld + .sha256 hash to the respective subfolders.

    Args:
        df (pd.DataFrame): The entire dataset in tabular form.
        variable_metadata (dict): column => metadata dictionary.
        ns_map (dict): prefix => rdflib.Namespace
        user_chosen_prefix (str): e.g. 'mds'
        dataset_name (str): e.g. 'SampleDataset' (already sanitized or not).
        orcid (str): The user's ORCID from which we parse digits for naming.
        output_folder_paths (dict): subfolders => their paths.
        fixed_dimensions (list or None): optionally specify columns that remain fixed in every slice.
        overall_timestamp (str or None): If provided, used for consistent naming across slices.

    Returns:
        None
    """
    from rdflib import Graph
    user_ns = ns_map[user_chosen_prefix]

    candidate_id_cols = [c for c in df.columns if re.search(r'id', c, re.IGNORECASE)]
    approved_id_cols = get_approved_id_columns(candidate_id_cols, mode='entire')

    safe_iri_name  = _sanitize_for_iri(dataset_name)
    safe_file_name = _sanitize_for_filename(dataset_name)

    dimensions, measures = extract_variables(variable_metadata, df.columns)
    if EXPERIMENT_ID_COLUMN in df.columns and EXPERIMENT_ID_COLUMN not in dimensions:
        dimensions.insert(0, EXPERIMENT_ID_COLUMN)

    dsd_graph, dsd_uri = create_dsd(variable_metadata, dimensions, measures, ns_map, user_ns)

    entire_graph = Graph()
    for prefix, ns_obj in ns_map.items():
        entire_graph.bind(prefix, ns_obj)
    entire_graph.bind('skos', SKOS)

    # copy the DSD
    for triple in dsd_graph:
        entire_graph.add(triple)

    dataset_uri_str = _sanitize_for_iri(f"Dataset_{safe_iri_name}")
    dataset_uri = user_ns[dataset_uri_str]
    entire_graph.add((dataset_uri, RDF.type, QB.DataSet))
    entire_graph.add((dataset_uri, QB.structure, dsd_uri))
    entire_graph.add((dataset_uri, DCTERMS.title, Literal(dataset_name)))
    entire_graph.add((dataset_uri, DCTERMS.creator, Literal(orcid)))

    global_slice_key_str = _sanitize_for_iri(f"SliceKey_{safe_iri_name}")
    global_slice_key_uri = user_ns[global_slice_key_str]
    entire_graph.add((global_slice_key_uri, RDF.type, QB.SliceKey))

    if fixed_dimensions is None:
        fixed_dimensions = dimensions

    for dim_name in fixed_dimensions:
        dim_prop = get_property_uri(dim_name, variable_metadata[dim_name], ns_map, user_ns)
        entire_graph.add((global_slice_key_uri, QB.componentProperty, dim_prop))

    if not overall_timestamp:
        overall_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    numeric_orcid = ''.join(re.findall(r'\d+', orcid))

    observation_counter = 1
    not_found_uri = user_ns['NotFound']

    # Build slices for each row
    for idx, row in df.iterrows():
        name_parts_for_iri = []
        for col in approved_id_cols:
            val = row[col] if pd.notnull(row[col]) else "NotFound"
            name_parts_for_iri.append(_sanitize_for_iri(str(val)))

        if any(name_parts_for_iri):
            slice_key_iri = "_".join(name_parts_for_iri + [numeric_orcid, overall_timestamp])
        else:
            slice_key_iri = f"{numeric_orcid}_{overall_timestamp}"
        slice_key_iri = _sanitize_for_iri(slice_key_iri)

        slice_id_str = _sanitize_for_iri(f"Slice_{slice_key_iri}")
        slice_uri = user_ns[slice_id_str]

        entire_graph.add((slice_uri, RDF.type, QB.Slice))
        entire_graph.add((slice_uri, QB.sliceStructure, global_slice_key_uri))
        entire_graph.add((dataset_uri, QB.slice, slice_uri))

        for dim_name in fixed_dimensions:
            dim_val = row.get(dim_name)
            dim_meta = variable_metadata[dim_name]
            dim_prop = get_property_uri(dim_name, dim_meta, ns_map, user_ns)
            if pd.notnull(dim_val):
                entire_graph.add((slice_uri, dim_prop, Literal(dim_val)))
            else:
                entire_graph.add((slice_uri, dim_prop, not_found_uri))

        variable_dimensions = [d for d in dimensions if d not in fixed_dimensions]
        obs_list, observation_counter = create_observation(
            entire_graph,
            row,
            variable_metadata,
            variable_dimensions,
            measures,
            ns_map,
            user_ns,
            observation_counter
        )
        for obs_uri in obs_list:
            entire_graph.add((slice_uri, QB.observation, obs_uri))

    # Write out single TTL/JSON-LD + hash
    ttl_path = os.path.join(output_folder_paths["ttl"], f"{safe_file_name}.ttl")
    jsonld_path = os.path.join(output_folder_paths["jsonld"], f"{safe_file_name}.jsonld")
    hash_path = os.path.join(output_folder_paths["hash"], f"{safe_file_name}.jsonld.sha256")

    entire_graph.serialize(destination=ttl_path, format='turtle')
    entire_graph.serialize(destination=jsonld_path, format='json-ld', auto_compact=True)

    file_hash = compute_file_hash(jsonld_path)
    with open(hash_path, 'w') as hash_file:
        hash_file.write(file_hash)



# =============================================================================
#           MASTER FUNCTION
# =============================================================================

def convert_dataset_to_rdf_with_mode(
    df: pd.DataFrame,
    variable_metadata: dict,
    namespace_map: dict,
    user_chosen_prefix: str = DEFAULT_USER_PREFIX,
    output_folder_path: str = '.',
    orcid: str = '',
    dataset_name: str = DEFAULT_DATASET_NAME,
    fixed_dimensions: list = None,
    conversion_mode: str = 'entire'
) -> None:
    """
    Description:
        Main entry point for converting a Pandas DataFrame to RDF using either:
         - 'entire': single qb:DataSet with multiple qb:Slices
         - 'row-by-row': each row => a separate qb:DataSet
        Also writes a naming conventions .txt file describing how URIs and filenames are formed.

    Algorithm:
        1) create_root_folder => get a top-level folder named "Output_{orcidDigits}_{timestamp}".
        2) prepare_namespaces => validate prefix => namespace URIs.
        3) If conversion_mode == 'entire':
             a) create_subfolders => 'ttl', 'jsonld', 'hash'
             b) build combined_iri => dataset_name_for_iri
             c) call convert_entire_dataset(...)
           elif conversion_mode == 'row-by-row':
             call convert_row_by_row(...)
           else:
             raise ValueError if mode is invalid
        4) write_naming_conventions_doc => describing the chosen naming approach
        5) Print success message.

    Args:
        df (pd.DataFrame): The data to convert.
        variable_metadata (dict): column => metadata (like IsMeasure, Unit, Category, etc.).
        namespace_map (dict): prefix => base URI for RDF
        user_chosen_prefix (str): e.g. 'mds'
        output_folder_path (str): base folder path for new output folder
        orcid (str): user’s ORCID
        dataset_name (str): used if 'entire' mode
        fixed_dimensions (list or None): used in entire mode to specify columns that
                                         remain the same across slices
        conversion_mode (str): 'entire' or 'row-by-row'

    Returns:
        None
    """
    try:
        # 1) create the root folder
        root_folder_path, overall_timestamp, sanitized_dataset_name, sanitized_orcid = create_root_folder(
            output_folder_path, dataset_name, orcid
        )

        # 2) prepare namespaces
        ns_map = prepare_namespaces(namespace_map, user_chosen_prefix)

        # 3) entire or row-by-row
        if conversion_mode == 'entire':
            subfolders = create_subfolders(root_folder_path)
            combined_iri = f"{sanitized_dataset_name}_{sanitized_orcid}_{overall_timestamp}"
            dataset_name_for_iri = _sanitize_for_iri(combined_iri)

            convert_entire_dataset(
                df=df,
                variable_metadata=variable_metadata,
                ns_map=ns_map,
                user_chosen_prefix=user_chosen_prefix,
                dataset_name=dataset_name_for_iri,
                orcid=orcid,
                output_folder_paths=subfolders,
                fixed_dimensions=fixed_dimensions,
                overall_timestamp=overall_timestamp
            )

        elif conversion_mode == 'row-by-row':
            convert_row_by_row(
                df=df,
                variable_metadata=variable_metadata,
                ns_map=ns_map,
                user_chosen_prefix=user_chosen_prefix,
                orcid=orcid,
                root_folder_path=root_folder_path,
                overall_timestamp=overall_timestamp
            )

        elif conversion_mode == "CRADLE":
            convert_row_by_row_CRADLE(
                df=df,
                variable_metadata=variable_metadata,
                ns_map=ns_map,
                user_chosen_prefix=user_chosen_prefix,
                orcid=orcid,
                root_folder_path=root_folder_path,
                overall_timestamp=overall_timestamp
            )

        else:
            raise ValueError("Invalid conversion_mode. Choose 'entire' or 'row-by-row'.")

        # 4) Write naming conventions doc
        write_naming_conventions_doc(
             root_folder_path=root_folder_path,
             conversion_mode=conversion_mode,
             orcid=orcid,
             overall_timestamp=overall_timestamp,
             dataset_name=sanitized_dataset_name
         )

        print(
            f"Conversion completed under mode='{conversion_mode}'. "
            f"Outputs in: {root_folder_path}"
        )

    except Exception as e:
        print(f"An error occurred during RDF conversion: {e}")
        traceback.print_exc()