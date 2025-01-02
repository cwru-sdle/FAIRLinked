import os
import json
import re
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, SKOS, DCTERMS, XSD
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from FAIRLinked.utility import NAMESPACE_MAP

def parse_rdf_to_df(file_path: str,
                    variable_metadata_json_path: str,
                    arrow_output_path: str) -> tuple:
    """
    Description:
        Parses one or multiple RDF Data Cube file(s) (TTL or JSON-LD) into a single
        Pandas DataFrame plus a consolidated variable_metadata dictionary. This function
        supports both "row-by-row" style RDF (each row => separate qb:DataSet) and "entire"
        style RDF (one qb:DataSet with many slices), as well as any mixture of them 
        (multiple DataSets across multiple files).

        After parsing each file's DataSets, it merges the partial DataFrames and merges
        partial metadata:
         - Merges units from different Observations
         - Merges altLabels, categories, and measure/dimension flags
        Then sorts the resulting DataFrame and writes:
         1) The final DataFrame => Parquet
         2) The final variable_metadata => JSON
        Finally, prints summary stats and previews the first row.

    Algorithm (High-Level):
        1. Gather all valid RDF files (.ttl/.jsonld/.json-ld) from either a single file path
           or a directory (recursively).
        2. Initialize an empty list of partial DataFrames (all_dfs) and an empty dictionary
           for final_variable_metadata.
        3. For each RDF file:
            a. Determine the rdflib parse format ('turtle' or 'json-ld').
            b. Parse the Graph.
            c. Pass the Graph to _parse_single_rdf_graph(...) which may produce:
               (partial_df, partial_metadata).
            d. Concatenate partial_df to the global list (if not empty).
            e. Merge partial_metadata into final_variable_metadata, unifying 
               measure units, altLabels, categories, etc.
        4. Concatenate all partial DataFrames if any => final_df.
        5. Sort final_df by "ExperimentId" if present.
        6. Reorder columns by (Category, ColumnName), with "ExperimentId" forced to front if it exists.
        7. Convert final_df => PyArrow Table => Parquet => arrow_output_path.
        8. Dump final_variable_metadata => JSON => variable_metadata_json_path.
        9. Print summary stats & preview.

    Args:
        file_path (str):
            Path to either a single .ttl/.jsonld file or a folder containing multiple .ttl/.jsonld files.
        variable_metadata_json_path (str):
            Destination to write the final variable_metadata as JSON.
        arrow_output_path (str):
            Destination to write the final PyArrow Table (saved in Parquet format).

    Returns:
        (pa.Table, dict):
            pa.Table  => The final table of observations, after merging across all files.
            dict      => The final merged variable_metadata mapping each column => metadata.
    """
    # 1) Collect RDF files
    rdf_files = _collect_rdf_files(file_path)
    if not rdf_files:
        raise ValueError(f"No RDF files (.ttl, .jsonld, .json-ld) found in '{file_path}'")

    # We'll store partial DataFrames + partial metadata from each file
    all_dfs = []
    final_variable_metadata = {}

    # 2) Parse each RDF file
    for f in rdf_files:
        rdf_format = _guess_rdf_format(f)
        print(f"\nParsing file: {f} as {rdf_format} ...")

        # Parse graph
        g = Graph()
        g.parse(source=f, format=rdf_format)

        # Each file can contain multiple qb:DataSets, gather them
        partial_df, partial_metadata = _parse_single_rdf_graph(g)

        # if partial_df has data => accumulate
        if partial_df is not None and not partial_df.empty:
            all_dfs.append(partial_df)

        # unify partial_metadata => final_variable_metadata
        for var_name, pm in partial_metadata.items():
            if var_name not in final_variable_metadata:
                final_variable_metadata[var_name] = pm
            else:
                # unify measure units
                existing_units = set(final_variable_metadata[var_name].get("Unit", []))
                new_units = set(pm.get("Unit", []))
                final_variable_metadata[var_name]["Unit"] = sorted(existing_units.union(new_units))

                # unify altLabel, category, IsMeasure
                if (not final_variable_metadata[var_name].get("AltLabel")
                    and pm.get("AltLabel")):
                    final_variable_metadata[var_name]["AltLabel"] = pm["AltLabel"]
                if (not final_variable_metadata[var_name].get("Category")
                    and pm.get("Category")):
                    final_variable_metadata[var_name]["Category"] = pm["Category"]
                if (final_variable_metadata[var_name].get("IsMeasure","No")=="No"
                    and pm.get("IsMeasure","No")=="Yes"):
                    final_variable_metadata[var_name]["IsMeasure"] = "Yes"
                # ExistingURI generally consistent, so we won't overwrite

    # 2b) Combine all partial DataFrames
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    # 3) Sort final_df by 'ExperimentId' if present
    if "ExperimentId" in final_df.columns:
        final_df.sort_values(by="ExperimentId", inplace=True)

    # 4) Reorder columns by (Category, ColumnName), ensuring 'ExperimentId' is front if present
    var_categories = {
        vn: (final_variable_metadata[vn].get("Category") or "")
        for vn in final_variable_metadata
    }
    all_cols = list(final_df.columns)

    if "ExperimentId" in all_cols:
        all_cols.remove("ExperimentId")

    # Sort by category, then colName
    all_cols.sort(key=lambda c: (var_categories.get(c, ""), c))

    if "ExperimentId" in final_df.columns:
        final_cols = ["ExperimentId"] + all_cols
    else:
        final_cols = all_cols

    final_df = final_df[final_cols]

    # 5) Convert => PyArrow
    final_table = pa.Table.from_pandas(final_df, preserve_index=False)

    # 6) Save variable_metadata => JSON
    with open(variable_metadata_json_path, "w", encoding="utf-8") as outf:
        json.dump(final_variable_metadata, outf, indent=2, ensure_ascii=False)

    # 7) Save table => Parquet
    pq.write_table(final_table, arrow_output_path)

    # 8) Print final stats & preview
    _print_final_stats_and_preview(final_df, var_categories, rdf_files, file_path)

    return final_table, final_variable_metadata


# ------------------------------------------------------------------------------
#               PARSING ONE RDF GRAPH => DataFrame, Metadata
# ------------------------------------------------------------------------------

def _parse_single_rdf_graph(graph: Graph) -> tuple:
    """
    Description:
        Given a single rdflib.Graph that may contain multiple qb:DataSet URIs,
        parse each DataSet, gather partial DataFrames, unify partial metadata,
        and combine them into a single DataFrame + metadata for this one Graph.

    Algorithm:
        1) Identify all qb:DataSet URIs in graph.
        2) For each qb:DataSet => find qb:structure => parse dims/measures => partial_meta
        3) Also parse all slices & observations => partial DataFrame
        4) Merge partial DataFrames => merged_df
        5) Merge partial metadata => partial_meta_master

    Args:
        graph (Graph): The rdflib.Graph loaded from a single RDF file.

    Returns:
        (pd.DataFrame, dict):
            DataFrame => combined data from all qb:DataSets in this file
            dict => combined variable_metadata from these DataSets
    """
    # The URIRef for qb:DataSet
    qb_dataset_uri = URIRef(NAMESPACE_MAP['qb'] + "DataSet")
    datasets = set(graph.subjects(RDF.type, qb_dataset_uri))
    if not datasets:
        # no dataset => skip
        return pd.DataFrame(), {}

    partial_frames = []
    partial_meta_master = {}

    # For each dataset found
    for ds in datasets:
        qb_structure_uri = URIRef(NAMESPACE_MAP['qb'] + "structure")
        dsd_uri_list = list(graph.objects(ds, qb_structure_uri))
        if not dsd_uri_list:
            # dataset with no qb:structure => skip
            continue
        dsd_uri = dsd_uri_list[0]

        # parse dimension & measure definitions => partial_meta
        dims, meas, partial_meta = _extract_dims_meas_from_dsd(graph, dsd_uri)

        # unify partial_meta => partial_meta_master
        for var_name, pm in partial_meta.items():
            if var_name not in partial_meta_master:
                partial_meta_master[var_name] = pm
            else:
                # unify measure units
                existing_units = set(partial_meta_master[var_name].get("Unit", []))
                new_units = set(pm.get("Unit", []))
                partial_meta_master[var_name]["Unit"] = sorted(existing_units.union(new_units))

                if (not partial_meta_master[var_name].get("AltLabel")
                    and pm.get("AltLabel")):
                    partial_meta_master[var_name]["AltLabel"] = pm["AltLabel"]
                if (not partial_meta_master[var_name].get("Category")
                    and pm.get("Category")):
                    partial_meta_master[var_name]["Category"] = pm["Category"]
                if (partial_meta_master[var_name].get("IsMeasure","No")=="No"
                    and pm.get("IsMeasure","No")=="Yes"):
                    partial_meta_master[var_name]["IsMeasure"] = "Yes"
                # ExistingURI presumably consistent

        # parse slices + observations => partial_df
        partial_df = _extract_data_for_dataset(graph, ds, dims, meas, partial_meta)
        if partial_df is not None and not partial_df.empty:
            partial_frames.append(partial_df)

    # combine partial_frames => merged_df
    if partial_frames:
        merged_df = pd.concat(partial_frames, ignore_index=True)
    else:
        merged_df = pd.DataFrame()

    return merged_df, partial_meta_master


def _extract_dims_meas_from_dsd(graph: Graph, dsd_uri: URIRef) -> tuple:
    """
    Description:
        For a qb:DataStructureDefinition, find dimension and measure definitions.
        Build partial variable_metadata with "IsMeasure", "AltLabel", "Category", "Unit" (empty),
        "ExistingURI".  Skip qb:measureType dimension.

    Algorithm:
        1) Identify all qb:component blank nodes from dsd_uri.
        2) Each component may have either qb:dimension or qb:measure.
        3) If it's dimensionProperty => add to dim_list
           If measureProperty => add to meas_list
        4) altLabel => partial_meta[var_name]["AltLabel"], 
           category => partial_meta[var_name]["Category"]
        5) Return (dim_list, meas_list, partial_meta).

    Args:
        graph (Graph)
        dsd_uri (URIRef)
    Returns:
        (dimensions (list), measures (list), partial_variable_metadata (dict))
    """
    measure_type_uri_str = NAMESPACE_MAP['qb'] + "measureType"
    qb_comp_uri = URIRef(NAMESPACE_MAP['qb'] + "component")
    qb_dim_prop = URIRef(NAMESPACE_MAP['qb'] + "DimensionProperty")
    qb_meas_prop = URIRef(NAMESPACE_MAP['qb'] + "MeasureProperty")

    dim_list = []
    meas_list = []
    var_meta = {}

    # For each qb:component triple => usually comp_bnode
    for comp_bnode in graph.objects(dsd_uri, qb_comp_uri):
        dimension_prop = None
        measure_prop = None

        # check if comp_bnode has qb:dimension or qb:measure
        for p, o in graph.predicate_objects(comp_bnode):
            if str(p) == (NAMESPACE_MAP['qb'] + "dimension"):
                dimension_prop = o
            elif str(p) == (NAMESPACE_MAP['qb'] + "measure"):
                measure_prop = o

        prop_uri = dimension_prop if dimension_prop else measure_prop
        if not prop_uri:
            continue

        # skip measureType dimension
        if str(prop_uri) == measure_type_uri_str:
            continue

        # see if prop_uri is dimension or measure
        prop_types = list(graph.objects(prop_uri, RDF.type))
        var_name = _uri_to_var_name(prop_uri)

        if qb_dim_prop in prop_types:
            dim_list.append(var_name)
            is_measure_str = "No"
        elif qb_meas_prop in prop_types:
            meas_list.append(var_name)
            is_measure_str = "Yes"
        else:
            # unknown
            continue

        # altLabel if any
        alt_label_obj = list(graph.objects(prop_uri, SKOS.altLabel))
        alt_label = str(alt_label_obj[0]) if alt_label_obj else None

        # category if any
        cat_prop = URIRef(NAMESPACE_MAP['mds'] + "category")
        cat_obj = list(graph.objects(prop_uri, cat_prop))
        category_val = None
        if cat_obj:
            cat_str = str(cat_obj[0])
            category_val = cat_str.split('#')[-1].split('/')[-1]

        var_meta[var_name] = {
            "IsMeasure": is_measure_str,
            "AltLabel": alt_label,
            "Category": category_val,
            "Unit": [],
            "ExistingURI": str(prop_uri)
        }

    return (dim_list, meas_list, var_meta)


def _extract_data_for_dataset(
    graph: Graph,
    dataset_uri: URIRef,
    dimensions: list,
    measures: list,
    variable_metadata: dict
) -> pd.DataFrame:
    """
    Description:
        For a single qb:DataSet (URI), gather all qb:Slice URIs => gather Observations => 
        build a wide DataFrame. Each row in the DataFrame corresponds to a unique combination
        of dimension values (keys), storing each measure in columns. Also captures unit references.

    Algorithm:
        1) Find slices via (dataset_uri, qb:slice, slice_uri).
        2) For each slice => gather Observations => map observation => slice.
        3) For each Observation:
            a) identify measure property (via measureType).
            b) gather measure value from (observation, measureProp).
            c) gather dimension values from slice, store in dim_values dict.
            d) record in dimension_grouped_data[dimension_key][measure_name] = measure_value
            e) capture unit if sdmx-attribute:unitMeasure is present
        4) Convert dimension_grouped_data => pd.DataFrame.

    Args:
        graph (Graph): The RDF graph for the entire file.
        dataset_uri (URIRef): The qb:DataSet node
        dimensions (list): dimension column names
        measures (list): measure column names
        variable_metadata (dict): partial metadata to update with any discovered units

    Returns:
        pd.DataFrame: wide-format of dimension + measure columns. May be empty if no data found.
    """
    qb_slice_uri = URIRef(NAMESPACE_MAP['qb'] + "slice")
    qb_observation_uri = URIRef(NAMESPACE_MAP['qb'] + "observation")
    measure_type_uri = URIRef(NAMESPACE_MAP['qb'] + "measureType")

    # gather slices in this dataset
    slices_in_dataset = []
    for s in graph.objects(dataset_uri, qb_slice_uri):
        slices_in_dataset.append(s)

    # map observation => slice
    obs_to_slice = {}
    for sl in slices_in_dataset:
        for obs in graph.objects(sl, qb_observation_uri):
            obs_to_slice[obs] = sl

    dimension_grouped_data = {}
    unit_measure_uri = URIRef(NAMESPACE_MAP['sdmx-attribute'] + "unitMeasure")

    for obs in obs_to_slice:
        # find measure property => measure_name
        measure_prop = None
        for mo in graph.objects(obs, measure_type_uri):
            measure_prop = mo
            break
        if not measure_prop:
            continue

        measure_name = _uri_to_var_name(measure_prop)
        if measure_name not in measures:
            # skip unknown measure
            continue

        # measure_value
        measure_val = None
        for p, val in graph.predicate_objects(obs):
            if p == measure_prop:
                measure_val = val.toPython() if isinstance(val, Literal) else str(val)
                break
        if measure_val is None:
            continue

        # dimension values from slice
        slice_uri = obs_to_slice[obs]
        dim_values = {}
        if slice_uri:
            for dim_name in dimensions:
                dim_uri = URIRef(variable_metadata[dim_name]["ExistingURI"])
                for obj in graph.objects(slice_uri, dim_uri):
                    dim_values[dim_name] = obj.toPython() if isinstance(obj, Literal) else str(obj)

        # store in dimension_grouped_data
        dim_key = tuple((dn, dim_values.get(dn)) for dn in dimensions)
        if dim_key not in dimension_grouped_data:
            dimension_grouped_data[dim_key] = dim_values.copy()

        dimension_grouped_data[dim_key][measure_name] = measure_val

        # capture observation-level unit if any
        for obj in graph.objects(obs, unit_measure_uri):
            unit_val = str(obj)
            if "Unit" in variable_metadata[measure_name]:
                if unit_val not in variable_metadata[measure_name]["Unit"]:
                    variable_metadata[measure_name]["Unit"].append(unit_val)

    if dimension_grouped_data:
        data_rows = list(dimension_grouped_data.values())
        df = pd.DataFrame(data_rows)
    else:
        df = pd.DataFrame()

    return df


# ------------------------------------------------------------------------------
#                    UTILITY: FILE & FORMAT
# ------------------------------------------------------------------------------

def _collect_rdf_files(file_path: str) -> list:
    """
    Description:
        Collects all .ttl/.jsonld/.json-ld files from 'file_path' if it's a directory
        (recursively), or just checks if 'file_path' is one valid file.

    Algorithm:
        1) If directory => recursively os.walk, gather matching files.
        2) If single file => check extension, add if valid.
        3) Return list of absolute file paths found.

    Args:
        file_path (str): either a directory or single file path

    Returns:
        list of str: The matched RDF file paths
    """
    valid_exts = ('.ttl', '.jsonld', '.json-ld')
    found = []

    if os.path.isdir(file_path):
        # walk recursively
        for root, dirs, files in os.walk(file_path):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext in valid_exts:
                    found.append(os.path.join(root, fname))
    else:
        # single file
        ext = os.path.splitext(file_path)[1].lower()
        if ext in valid_exts:
            found.append(os.path.abspath(file_path))

    return found


def _guess_rdf_format(file_path: str) -> str:
    """
    Description:
        Guesses the RDF format for rdflib.parse based on extension.

    Returns 'turtle' for .ttl, 'json-ld' for .jsonld/.json-ld, else 'turtle'.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.ttl':
        return 'turtle'
    elif ext in ('.jsonld', '.json-ld'):
        return 'json-ld'
    return 'turtle'


def _uri_to_var_name(uri_val) -> str:
    """
    Description:
        Splits a URI on '#' or '/', returning the last part as the variable name.
        e.g. http://example.org#ExperimentId => "ExperimentId"
    """
    uri_str = str(uri_val)
    part = uri_str.split('#')[-1]
    part = part.split('/')[-1]
    return part


# ------------------------------------------------------------------------------
#                PRINTING FINAL STATS & PREVIEW
# ------------------------------------------------------------------------------

def _print_final_stats_and_preview(df: pd.DataFrame,
                                   var_categories: dict,
                                   rdf_files: list,
                                   file_path: str) -> None:
    """
    Description:
        Prints a summary of the final merged DataFrame: number of rows, columns,
        distinct categories, plus a preview of the first row if any data.

    Args:
        df (pd.DataFrame): The final merged DataFrame.
        var_categories (dict): column => category
        rdf_files (list): The list of all parsed RDF file paths
        file_path (str): The original user-supplied path (file or folder).
    """
    num_rows = len(df)
    num_cols = len(df.columns)

    # gather categories for columns that exist in df
    distinct_categories = set()
    for col in df.columns:
        ccat = var_categories.get(col, "")
        if ccat:
            distinct_categories.add(ccat)

    print("\n=== Final Conversion Stats ===")
    if len(rdf_files) == 1:
        print(f"Source: Single RDF file => {rdf_files[0]}")
    else:
        print(f"Source: {len(rdf_files)} RDF files from => {file_path}")

    print(f"Total Rows (Experiments): {num_rows}")
    print(f"Total Columns (Variables): {num_cols}")

    if distinct_categories:
        cats_sorted = sorted(distinct_categories)
        print(f"Distinct Categories Found: {len(cats_sorted)} ({', '.join(cats_sorted)})")
    else:
        print("Distinct Categories Found: 0")

    if num_rows > 0:
        print("\n=== First Row Preview ===")
        print(df.iloc[0].to_dict())
    else:
        print("\nNo data rows found.")