import os
import json
import re
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, SKOS, DCTERMS, XSD
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from packages.FAIRLinkedPy.FAIRLinked.utility import NAMESPACE_MAP


def _guess_rdf_format(filename):
    """
    Guess the RDF serialization format based on file extension.

    Args:
        filename (str): Path to the RDF file.

    Returns:
        str: RDF format string (e.g., 'turtle', 'json-ld').

    Raises:
        ValueError: If file extension is unrecognized.
    """
    if filename.lower().endswith('.ttl'):
        return "turtle"
    elif filename.lower().endswith(('.jsonld', '.json-ld')):
        return "json-ld"
    else:
        raise ValueError(f"Unknown RDF file extension for: {filename}")


def _parse_single_rdf_graph(g):
    """
    Convert an RDF graph to a pivoted DataFrame (wide format).

    Args:
        g (rdflib.Graph): RDF graph to parse.

    Returns:
        tuple:
            - pd.DataFrame: Pivoted DataFrame with subject as index.
            - dict: Placeholder for variable metadata (currently empty).
    """
    rows = []
    for s, p, o in g:
        rows.append({"subject": str(s), "predicate": str(p), "object": str(o)})

    df = pd.DataFrame(rows)
    if not df.empty:
        df_pivot = df.pivot_table(
            index='subject',
            columns='predicate',
            values='object',
            aggfunc=lambda x: x.tolist()
        )
        df_pivot.reset_index(inplace=True)
    else:
        df_pivot = df

    return df_pivot, {}


def parse_rdf_to_df(file_path: str,
                    variable_metadata_json_path: str,
                    arrow_output_path: str) -> tuple:
    """
    Parse one or more RDF files into a combined DataFrame, generate variable metadata,
    and export the results in Parquet, JSON, CSV, and Excel formats.

    Args:
        file_path (str): RDF file or directory path.
        variable_metadata_json_path (str): Output path for variable metadata JSON.
        arrow_output_path (str): Output path for the Parquet table.

    Returns:
        tuple:
            - pa.Table: Final Arrow table.
            - dict: Final variable metadata.
    """
    rdf_files = _collect_rdf_files(file_path)
    if not rdf_files:
        raise ValueError(f"No RDF files (.ttl, .jsonld, .json-ld) found in '{file_path}'")

    all_dfs = []
    final_variable_metadata = {}

    for f in rdf_files:
        rdf_format = _guess_rdf_format(f)
        print(f"\nParsing file: {f} as {rdf_format} ...")

        g = Graph()
        g.parse(source=f, format=rdf_format)

        partial_df, partial_metadata = _parse_single_rdf_graph(g)

        if partial_df is not None and not partial_df.empty:
            all_dfs.append(partial_df)

        for var_name, pm in partial_metadata.items():
            if var_name not in final_variable_metadata:
                final_variable_metadata[var_name] = pm
            else:
                existing_units = set(final_variable_metadata[var_name].get("Unit", []))
                new_units = set(pm.get("Unit", []))
                final_variable_metadata[var_name]["Unit"] = sorted(existing_units.union(new_units))

                if (not final_variable_metadata[var_name].get("AltLabel")
                        and pm.get("AltLabel")):
                    final_variable_metadata[var_name]["AltLabel"] = pm["AltLabel"]
                if (not final_variable_metadata[var_name].get("Category")
                        and pm.get("Category")):
                    final_variable_metadata[var_name]["Category"] = pm["Category"]
                if (final_variable_metadata[var_name].get("IsMeasure", "No") == "No"
                        and pm.get("IsMeasure", "No") == "Yes"):
                    final_variable_metadata[var_name]["IsMeasure"] = "Yes"

    final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    if "ExperimentId" in final_df.columns:
        final_df.sort_values(by="ExperimentId", inplace=True)

    var_categories = {
        vn: (final_variable_metadata[vn].get("Category") or "")
        for vn in final_variable_metadata
    }
    all_cols = list(final_df.columns)
    if "ExperimentId" in all_cols:
        all_cols.remove("ExperimentId")
    all_cols.sort(key=lambda c: (var_categories.get(c, ""), c))
    final_cols = ["ExperimentId"] + all_cols if "ExperimentId" in final_df.columns else all_cols
    final_df = final_df[final_cols]

    final_table = pa.Table.from_pandas(final_df, preserve_index=False)

    with open(variable_metadata_json_path, "w", encoding="utf-8") as outf:
        json.dump(final_variable_metadata, outf, indent=2, ensure_ascii=False)

    if variable_metadata_json_path.lower().endswith(".json"):
        variable_metadata_csv_path = variable_metadata_json_path.replace(".json", ".csv")
        metadata_df = pd.DataFrame.from_dict(final_variable_metadata, orient="index")
        metadata_df.insert(0, "Variable", metadata_df.index)
        metadata_df.reset_index(drop=True, inplace=True)
        metadata_df.to_csv(variable_metadata_csv_path, index=False)
        print(f"✅ Saved metadata CSV to: {variable_metadata_csv_path}")

    pq.write_table(final_table, arrow_output_path)

    final_json_path = arrow_output_path.replace(".parquet", ".json")
    final_df.to_json(final_json_path, orient="records", indent=2)
    print(f"✅ Saved final DataFrame as JSON to: {final_json_path}")

    final_csv_path = arrow_output_path.replace(".parquet", ".csv")
    final_df.to_csv(final_csv_path, index=False)
    print(f"✅ Saved final DataFrame as CSV to: {final_csv_path}")

    final_excel_path = arrow_output_path.replace(".parquet", ".xlsx")
    final_df.to_excel(final_excel_path, index=False)
    print(f"✅ Saved final DataFrame as Excel to: {final_excel_path}")

    _print_final_stats_and_preview(final_df, var_categories, rdf_files, file_path)

    return final_table, final_variable_metadata


def _print_final_stats_and_preview(df: pd.DataFrame,
                                   var_categories: dict,
                                   rdf_files: list,
                                   file_path: str) -> None:
    """
    Print final dataset statistics and a preview row.

    Args:
        df (pd.DataFrame): Final DataFrame.
        var_categories (dict): Mapping from variable names to categories.
        rdf_files (list): List of RDF files parsed.
        file_path (str): Original input path.
    """
    num_rows = len(df)
    num_cols = len(df.columns)
    distinct_categories = set()

    for col in df.columns:
        cat = var_categories.get(col, "")
        if cat:
            distinct_categories.add(cat)

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


def _collect_rdf_files(file_path):
    """
    Collect RDF files from a single file or a directory.

    Args:
        file_path (str): Input file or directory path.

    Returns:
        list: List of RDF file paths (.ttl, .jsonld, .json-ld).
    """
    rdf_files = []
    if os.path.isfile(file_path):
        if file_path.lower().endswith(('.ttl', '.jsonld', '.json-ld')):
            return [file_path]
    elif os.path.isdir(file_path):
        for root, _, files in os.walk(file_path):
            for fname in files:
                if fname.lower().endswith(('.ttl', '.jsonld', '.json-ld')):
                    rdf_files.append(os.path.join(root, fname))
    return rdf_files
