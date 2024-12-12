import os
import hashlib
import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, XSD, DCTERMS
from datetime import datetime
import re
import traceback  # For detailed exception information

# Try importing QB from rdflib.namespace if available
try:
    from rdflib.namespace import QB
except ImportError:
    # If QB is not available, define it manually
    QB = Namespace('http://purl.org/linked-data/cube#')

# Optionally use SKOS for altLabels
from rdflib.namespace import SKOS

# Constants for error messages
ERROR_MSG_VARIABLE_NOT_FOUND = "Warning: Variable '{var_name}' not found in variable metadata. Skipping."
ERROR_MSG_PREFIX_NOT_FOUND = "Prefix '{prefix}' for unit '{unit_str}' not found in namespace map."

# Constants for column names and metadata fields
EXPERIMENT_ID_COLUMN = 'ExperimentId'  # This can be updated as needed

IS_MEASURE_FIELD = 'IsMeasure'
UNIT_FIELD = 'Unit'
CATEGORY_FIELD = 'Category'
EXISTING_URI_FIELD = 'ExistingURI'

DEFAULT_USER_PREFIX = 'mds'
DEFAULT_DATASET_NAME = 'SampleDataset'

def create_root_folder(output_folder_path: str, dataset_name: str, orcid: str) -> tuple:
    """
    Creates a timestamped root folder for the output files.

    Algorithm:
    1. Generate a timestamp.
    2. Sanitize dataset name and ORCID for file system safety.
    3. Create a directory named using sanitized dataset name, ORCID, and timestamp.

    Time Complexity: O(1)

    Args:
        output_folder_path (str): Base output folder path.
        dataset_name (str): Name of the dataset.
        orcid (str): ORCID identifier.

    Returns:
        tuple: (root_folder_path, timestamp, sanitized_dataset_name, sanitized_orcid)
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    sanitized_dataset_name = re.sub(r'\W|^(?=\d)', '_', dataset_name)
    sanitized_orcid = ''.join(re.findall(r'\d+', orcid))
    root_folder_name = f"{sanitized_dataset_name}_{sanitized_orcid}_{timestamp}"
    root_folder_path = os.path.join(output_folder_path, root_folder_name)
    os.makedirs(root_folder_path, exist_ok=True)
    return root_folder_path, timestamp, sanitized_dataset_name, sanitized_orcid

def prepare_namespaces(namespace_map: dict, user_chosen_prefix: str = DEFAULT_USER_PREFIX) -> dict:
    """
    Prepares namespaces by ensuring the user-chosen prefix is included and converting URIs to Namespace objects.

    Algorithm:
    1. Verify user prefix is in namespace_map.
    2. Validate each URI, ensure ends with '/' or '#', convert to Namespace.
    
    Time Complexity: O(N) where N is the number of namespaces.

    Args:
        namespace_map (dict): Mapping of prefixes to namespace URIs.
        user_chosen_prefix (str): User-chosen prefix.

    Returns:
        dict: Namespace objects keyed by prefixes.

    Raises:
        ValueError: If user prefix missing or URI invalid.
    """
    ns_map = namespace_map.copy()
    if user_chosen_prefix not in ns_map:
        raise ValueError(f"Namespace URI for prefix '{user_chosen_prefix}' required.")

    for prefix, uri in ns_map.items():
        if not re.match(r'^https?://', uri):
            raise ValueError(f"Invalid URI '{uri}' for prefix '{prefix}'.")
        if not uri.endswith(('/', '#')):
            ns_map[prefix] = uri + '#'

    ns_map = {prefix: Namespace(uri) for prefix, uri in ns_map.items()}
    return ns_map

def extract_variables(variable_metadata: dict, df_columns: list) -> tuple:
    """
    Extract dimensions and measures from variable_metadata, maintaining column order.

    Algorithm:
    1. Iterate df_columns.
    2. Check variable_metadata for each column.
    3. If IsMeasure == 'yes', it's a measure, else dimension.
    4. Missing metadata: print warning, skip.

    Time Complexity: O(C) where C is number of df columns.

    Args:
        variable_metadata (dict)
        df_columns (list)

    Returns:
        (dimensions (list), measures (list))
    """
    dimensions = []
    measures = []
    for var_name in df_columns:
        meta = variable_metadata.get(var_name)
        if meta:
            is_measure_value = meta.get(IS_MEASURE_FIELD)
            if isinstance(is_measure_value, str):
                is_measure = is_measure_value.strip().lower() == 'yes'
            else:
                is_measure = False
            if is_measure:
                measures.append(var_name)
            else:
                dimensions.append(var_name)
        else:
            print(ERROR_MSG_VARIABLE_NOT_FOUND.format(var_name=var_name))
    return dimensions, measures

def get_property_uri(var_name: str, meta: dict, ns_map: dict, user_ns: Namespace) -> URIRef:
    """
    Returns the URI for a variable, using ExistingURI if available; else use user namespace.

    Algorithm:
    1. Check ExistingURI.
       - If prefixed, resolve using ns_map.
       - If absolute URI, use as is.
       - Else user_ns[var_name].

    Time Complexity: O(1)

    Raises:
        ValueError if prefix not found.
    """
    existing_uri = meta.get(EXISTING_URI_FIELD)
    if existing_uri:
        if ':' in existing_uri:
            prefix, local_part = existing_uri.split(':', 1)
            ns = ns_map.get(prefix)
            if ns:
                return ns[local_part]
            else:
                raise ValueError(f"Prefix '{prefix}' not found for ExistingURI '{existing_uri}'")
        else:
            return URIRef(existing_uri)
    else:
        sanitized_var_name = var_name.replace(' ', '_')
        return user_ns[sanitized_var_name]

def process_unit(unit_str: str, ns_map: dict, user_ns: Namespace) -> URIRef:
    """
    Processes the unit string and returns a URI. If prefixed, resolve prefix. Else user_ns.

    Time Complexity: O(1)

    Missing unit_str returns None.
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

def add_component_to_dsd(dsd_graph: Graph, dsd_uri: URIRef, prop_uri: URIRef,
                         component_type: URIRef, prop_type: URIRef) -> None:
    """
    Adds a component to the DSD.

    Algorithm:
    1. Create blank node for component.
    2. Link DSD to component, and component to prop_uri with prop_type.

    Time Complexity: O(1)
    """
    component = BNode()
    dsd_graph.add((dsd_uri, QB.component, component))
    dsd_graph.add((component, component_type, prop_uri))
    dsd_graph.add((prop_uri, RDF.type, prop_type))

def create_dsd(variable_metadata: dict, dimensions: list, measures: list,
               ns_map: dict, user_ns: Namespace) -> tuple:
    """
    Creates the Data Structure Definition (DSD).

    Algorithm:
    1. Create DSD URI, add qb:DataStructureDefinition.
    2. Add dimension properties, measureType dimension, measure properties.
    3. Add altLabel and category if available on each property.
    4. Identify attributes (unitMeasure, category). If present in variables, add attribute components.

    Category now attached at the property level (like altLabel), not per observation.

    Time Complexity: O(D+M)

    Args:
        variable_metadata, dimensions, measures, ns_map, user_ns

    Returns:
        (dsd_graph, dsd_uri)
    """
    dsd_graph = Graph()
    for prefix, namespace in ns_map.items():
        dsd_graph.bind(prefix, namespace)
    dsd_graph.bind('skos', SKOS)

    dsd_uri = user_ns["DataStructureDefinition"]
    dsd_graph.add((dsd_uri, RDF.type, QB.DataStructureDefinition))

    # Add dimension components (excluding measureType)
    for var_name in dimensions:
        meta = variable_metadata[var_name]
        dimension_prop = get_property_uri(var_name, meta, ns_map, user_ns)
        add_component_to_dsd(dsd_graph, dsd_uri, dimension_prop, QB.dimension, QB.DimensionProperty)
        # altLabel if available
        alt_label = meta.get("AltLabel")
        if alt_label:
            dsd_graph.add((dimension_prop, SKOS.altLabel, Literal(alt_label)))
        # category if available (for dimension)
        category = meta.get(CATEGORY_FIELD)
        if category:
            category_prop = user_ns['category']
            category_uri = user_ns[category.replace(' ', '_')]
            dsd_graph.add((dimension_prop, category_prop, category_uri))

    # Add measureType as a dimension (Measure Dimension approach)
    add_component_to_dsd(dsd_graph, dsd_uri, QB.measureType, QB.dimension, QB.DimensionProperty)

    # Add measure components
    for var_name in measures:
        meta = variable_metadata[var_name]
        measure_prop = get_property_uri(var_name, meta, ns_map, user_ns)
        add_component_to_dsd(dsd_graph, dsd_uri, measure_prop, QB.measure, QB.MeasureProperty)
        # altLabel if available
        alt_label = meta.get("AltLabel")
        if alt_label:
            dsd_graph.add((measure_prop, SKOS.altLabel, Literal(alt_label)))
        # category if available (for measure)
        category = meta.get(CATEGORY_FIELD)
        if category:
            category_prop = user_ns['category']
            category_uri = user_ns[category.replace(' ', '_')]
            dsd_graph.add((measure_prop, category_prop, category_uri))

    # Identify if any attributes used (unitMeasure, category)
    attributes_used = set()
    for var_name in variable_metadata:
        meta = variable_metadata[var_name]
        if UNIT_FIELD in meta and meta[UNIT_FIELD]:
            attributes_used.add('unitMeasure')
        if CATEGORY_FIELD in meta and meta[CATEGORY_FIELD]:
            attributes_used.add('category')

    sdmx_attribute = ns_map.get('sdmx-attribute')
    if 'unitMeasure' in attributes_used and sdmx_attribute:
        # add unitMeasure attribute
        unit_measure_prop = sdmx_attribute['unitMeasure']
        add_component_to_dsd(dsd_graph, dsd_uri, unit_measure_prop, QB.attribute, QB.AttributeProperty)
    elif 'unitMeasure' in attributes_used and not sdmx_attribute:
        print("Warning: 'sdmx-attribute' namespace not found. Skipping unitMeasure attribute.")

    if 'category' in attributes_used:
        # category is a custom attribute at the property level
        # Already handled per property, but we can also add attribute definition
        category_prop = user_ns['category']
        add_component_to_dsd(dsd_graph, dsd_uri, category_prop, QB.attribute, QB.AttributeProperty)

    return dsd_graph, dsd_uri

def create_slice_key(dsd_graph: Graph, fixed_dimensions: list, variable_metadata: dict,
                     ns_map: dict, user_ns: Namespace) -> URIRef:
    """
    Creates a slice key for slices.

    Algorithm:
    1. Create SliceKey URI.
    2. Add componentProperty for each fixed dimension.

    Time Complexity: O(F)
    """
    slice_key = user_ns['ExperimentSliceKey']
    dsd_graph.add((slice_key, RDF.type, QB.SliceKey))
    for dim_name in fixed_dimensions:
        dim_prop = get_property_uri(dim_name, variable_metadata[dim_name], ns_map, user_ns)
        dsd_graph.add((slice_key, QB.componentProperty, dim_prop))
    return slice_key

def create_observation(dataset_graph: Graph, row: pd.Series, variable_metadata: dict,
                       variable_dimensions: list, measures: list, ns_map: dict,
                       user_ns: Namespace, observation_counter: int) -> tuple:
    """
    Creates observations for each row of data. 
    Note: Category is no longer added at observation-level.

    Algorithm:
    1. For each measure in the row: if measure value present, create Observation.
    2. Link Observation to measureType and measure property.
    3. Add dimension values or mds:NotFound if missing.
    4. Add unitMeasure if available. (Category is now attached at property-level in DSD, not here.)

    Time Complexity: O(M) per row

    Missing Data:
    - No measure value: no observation.
    - Missing dimension: mds:NotFound.

    Args:
        ... (see code)

    Returns:
        (observations (list), observation_counter)
    """
    sdmx_attribute = ns_map.get('sdmx-attribute')
    not_found_uri = user_ns['NotFound']

    observations = []
    for measure_name in measures:
        measure_value = row.get(measure_name)
        if pd.notnull(measure_value):
            meta = variable_metadata[measure_name]
            measure_prop = get_property_uri(measure_name, meta, ns_map, user_ns)
            observation_uri = user_ns[f"observation_{observation_counter}"]
            observation_counter += 1

            # Observation type
            dataset_graph.add((observation_uri, RDF.type, QB.Observation))
            # measureType
            dataset_graph.add((observation_uri, QB.measureType, measure_prop))
            # measure value
            try:
                value_literal = Literal(float(measure_value), datatype=XSD.double)
            except (ValueError, TypeError):
                value_literal = Literal(measure_value)
            dataset_graph.add((observation_uri, measure_prop, value_literal))

            # Dimensions
            for dimension_name in variable_dimensions:
                dimension_value = row.get(dimension_name)
                dimension_meta = variable_metadata[dimension_name]
                dimension_prop = get_property_uri(dimension_name, dimension_meta, ns_map, user_ns)
                if pd.notnull(dimension_value):
                    dataset_graph.add((observation_uri, dimension_prop, Literal(dimension_value)))
                else:
                    dataset_graph.add((observation_uri, dimension_prop, not_found_uri))

            # Unit if available at property level
            unit_uri = process_unit(meta.get(UNIT_FIELD), ns_map, user_ns)
            if unit_uri and sdmx_attribute:
                dataset_graph.add((observation_uri, sdmx_attribute['unitMeasure'], unit_uri))
            elif unit_uri and not sdmx_attribute:
                print("Warning: 'sdmx-attribute' namespace not found. Skipping unitMeasure attribute.")

            # Category no longer added here (now on the property-level in create_dsd).

            observations.append(observation_uri)
    return observations, observation_counter

def create_dataset_graph(df: pd.DataFrame, variable_metadata: dict, dimensions: list,
                         measures: list, ns_map: dict, user_ns: Namespace,
                         dataset_name: str, dsd_graph: Graph, dsd_uri: URIRef,
                         orcid: str, fixed_dimensions: list) -> Graph:
    """
    Creates the dataset graph including DSD, slices, and observations.

    Algorithm:
    1. Start with DSD graph.
    2. Create DataSet URI and add title, creator.
    3. Create slice key with fixed dimensions.
    4. Iterate rows, group by fixed dimensions to form slices.
    5. Add observations for each measure value in each row.
    6. Dimensions fixed at slice-level, measures at observation-level.
    7. Category and altLabel at property-level (already done in create_dsd).

    Time Complexity: O(R*M)

    Missing Data:
    - Entire missing column: dropped earlier.
    - Missing measure: no observation.
    - Missing dimension: mds:NotFound at slice-level.

    Raises ValueError if EXPERIMENT_ID_COLUMN not in fixed_dimensions.
    """
    dataset_graph = dsd_graph

    sanitized_dataset_name = dataset_name.replace(' ', '_')
    dataset_uri = user_ns[sanitized_dataset_name]
    dataset_graph.add((dataset_uri, RDF.type, QB.DataSet))
    dataset_graph.add((dataset_uri, QB.structure, dsd_uri))
    dataset_graph.add((dataset_uri, DCTERMS.title, Literal(dataset_name)))
    dataset_graph.add((dataset_uri, DCTERMS.creator, Literal(orcid)))

    slice_key = create_slice_key(dataset_graph, fixed_dimensions, variable_metadata, ns_map, user_ns)

    observation_counter = 1
    not_found_uri = user_ns['NotFound']
    slice_mappings = {}

    if EXPERIMENT_ID_COLUMN not in fixed_dimensions:
        raise ValueError(f"'{EXPERIMENT_ID_COLUMN}' must be in fixed_dimensions.")

    experiment_id_index = fixed_dimensions.index(EXPERIMENT_ID_COLUMN)

    for idx, row in df.iterrows():
        slice_key_values = []
        for dim_name in fixed_dimensions:
            dim_value = row.get(dim_name)
            if pd.notnull(dim_value):
                slice_key_values.append(str(dim_value))
            else:
                slice_key_values.append("NotFound")

        slice_key_tuple = tuple(slice_key_values)
        experiment_id_value = slice_key_values[experiment_id_index]
        sanitized_experiment_id = re.sub(r'\W|^(?=\d)', '_', experiment_id_value)

        slice_uri = user_ns[f"Slice_Experiment_{sanitized_experiment_id}"]

        if slice_key_tuple not in slice_mappings:
            slice_mappings[slice_key_tuple] = {'uri': slice_uri, 'observations': []}
            dataset_graph.add((slice_uri, RDF.type, QB.Slice))
            dataset_graph.add((slice_uri, QB.sliceStructure, slice_key))
            dataset_graph.add((dataset_uri, QB.slice, slice_uri))
            # Add fixed dimension values to slice
            for dim_name, dim_value_str in zip(fixed_dimensions, slice_key_values):
                dimension_meta = variable_metadata[dim_name]
                dimension_prop = get_property_uri(dim_name, dimension_meta, ns_map, user_ns)
                if dim_value_str == "NotFound":
                    dataset_graph.add((slice_uri, dimension_prop, not_found_uri))
                else:
                    dataset_graph.add((slice_uri, dimension_prop, Literal(dim_value_str)))

        # Create observations for this row
        variable_dimensions = [dim for dim in dimensions if dim not in fixed_dimensions]
        observations, observation_counter = create_observation(
            dataset_graph, row, variable_metadata, variable_dimensions, measures,
            ns_map, user_ns, observation_counter
        )

        for obs_uri in observations:
            dataset_graph.add((slice_uri, QB.observation, obs_uri))

    return dataset_graph

def compute_file_hash(file_path: str) -> str:
    """
    Computes SHA-256 hash of a file.

    Algorithm:
    1. Read file in chunks.
    2. Update hash digest per chunk.

    Time Complexity: O(F) where F is file size.

    Args:
        file_path: Path to file.

    Returns:
        str: SHA-256 hash
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as file:
        for byte_block in iter(lambda: file.read(4096), b''):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def convert_dataset_to_rdf(df: pd.DataFrame, variable_metadata: dict, namespace_map: dict,
                           user_chosen_prefix: str = DEFAULT_USER_PREFIX,
                           output_folder_path: str = '.', orcid: str = '',
                           dataset_name: str = DEFAULT_DATASET_NAME,
                           fixed_dimensions: list = None) -> None:
    """
    Converts a DataFrame to RDF Data Cube using the Measure Dimension approach.

    Algorithm:
    1. Create output directory.
    2. Prepare namespaces.
    3. Extract dimensions and measures from variable_metadata.
    4. Ensure ExperimentId is a dimension.
    5. Create DSD (adds altLabel and category at property level).
    6. Create dataset graph with slices and observations.
    7. Serialize to TTL and JSON-LD.
    8. Compute SHA256 hash.

    Category is now attached at the property (variable) level in DSD, not at observation level.

    Time Complexity:
    O(R*M), dominated by iteration through rows and measures.

    Missing Data:
    - Entirely missing columns dropped before this step.
    - Missing measure: no observation.
    - Missing dimension: NotFound used.
    - No category/unit: simply not added.

    Args:
        df, variable_metadata, namespace_map, etc.

    Returns:
        None
    """
    try:
        root_folder_path, timestamp, sanitized_dataset_name, sanitized_orcid = create_root_folder(
            output_folder_path, dataset_name, orcid
        )

        ns_map = prepare_namespaces(namespace_map, user_chosen_prefix)
        user_ns = ns_map[user_chosen_prefix]

        dimensions, measures = extract_variables(variable_metadata, df.columns)

        # Ensure 'ExperimentId' is dimension and at front if present
        if EXPERIMENT_ID_COLUMN in df.columns and EXPERIMENT_ID_COLUMN not in dimensions:
            dimensions.insert(0, EXPERIMENT_ID_COLUMN)

        dsd_graph, dsd_uri = create_dsd(variable_metadata, dimensions, measures, ns_map, user_ns)

        if fixed_dimensions is None:
            fixed_dimensions = dimensions

        dataset_graph = create_dataset_graph(
            df, variable_metadata, dimensions, measures, ns_map, user_ns,
            dataset_name, dsd_graph, dsd_uri, orcid, fixed_dimensions
        )

        base_filename = f"{sanitized_dataset_name}_{sanitized_orcid}_{timestamp}"
        dataset_filename = f"{base_filename}.ttl"
        dataset_filepath = os.path.join(root_folder_path, dataset_filename)
        dataset_graph.serialize(destination=dataset_filepath, format='turtle')

        jsonld_filename = f"{base_filename}.jsonld"
        jsonld_filepath = os.path.join(root_folder_path, jsonld_filename)
        dataset_graph.serialize(destination=jsonld_filepath, format='json-ld')

        ttl_hash = compute_file_hash(dataset_filepath)
        hash_filename = f"{dataset_filename}.sha256"
        hash_filepath = os.path.join(root_folder_path, hash_filename)
        with open(hash_filepath, 'w') as hash_file:
            hash_file.write(ttl_hash)

        print(f"Conversion completed. Output stored in {root_folder_path}")

    except Exception as e:
        print(f"An error occurred during conversion: {e}")
        traceback.print_exc()