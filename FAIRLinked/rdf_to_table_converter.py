import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rdflib import Graph
from .utility import NAMESPACE_MAP  # Ensure this exists in your package


def _guess_rdf_format(filename):
    if filename.lower().endswith('.ttl'):
        return "turtle"
    elif filename.lower().endswith(('.jsonld', '.json-ld')):
        return "json-ld"
    else:
        raise ValueError(f"Unknown RDF file extension: {filename}")


def _parse_single_rdf_graph(graph):
    rows = [{"subject": str(s), "predicate": str(p), "object": str(o)} for s, p, o in graph]
    df = pd.DataFrame(rows)
    if not df.empty:
        df_pivot = df.pivot_table(index='subject', columns='predicate', values='object', aggfunc=lambda x: x.tolist())
        df_pivot.reset_index(inplace=True)
    else:
        df_pivot = df
    return df_pivot, {}  # Metadata extraction placeholder


def parse_rdf_to_df(file_path, variable_metadata_json_path, arrow_output_path):
    """
    Convert RDF (.ttl/.jsonld) files to a unified DataFrame and save in multiple formats.

    Args:
        file_path (str): RDF file or directory.
        variable_metadata_json_path (str): Where to save metadata JSON (and CSV).
        arrow_output_path (str): Where to save Arrow/Parquet/CSV/Excel/JSON outputs.

    Returns:
        tuple: (Arrow Table, Variable Metadata Dictionary)
    """
    rdf_files = _collect_rdf_files(file_path)
    if not rdf_files:
        raise ValueError(f"No RDF files found in: {file_path}")

    all_dfs = []
    metadata = {}

    for f in rdf_files:
        rdf_format = _guess_rdf_format(f)
        print(f"\n🔍 Parsing: {f} ({rdf_format})")
        g = Graph()
        g.parse(f, format=rdf_format)

        df, meta = _parse_single_rdf_graph(g)
        if not df.empty:
            all_dfs.append(df)

        for var, md in meta.items():
            if var not in metadata:
                metadata[var] = md
            else:
                metadata[var]["Unit"] = sorted(set(metadata[var].get("Unit", [])).union(md.get("Unit", [])))
                for key in ["AltLabel", "Category", "IsMeasure"]:
                    if not metadata[var].get(key) and md.get(key):
                        metadata[var][key] = md[key]

    final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    if "ExperimentId" in final_df.columns:
        final_df.sort_values("ExperimentId", inplace=True)

    var_categories = {k: metadata[k].get("Category", "") for k in metadata}
    reordered = _reorder_columns(final_df, var_categories)

    final_table = pa.Table.from_pandas(reordered, preserve_index=False)

    _save_all_outputs(reordered, final_table, variable_metadata_json_path, arrow_output_path)
    _print_final_stats_and_preview(reordered, var_categories, rdf_files, file_path)

    return final_table, metadata


def _collect_rdf_files(path):
    rdf_exts = ('.ttl', '.jsonld', '.json-ld')
    if os.path.isfile(path) and path.lower().endswith(rdf_exts):
        return [path]
    rdf_files = []
    for root, _, files in os.walk(path):
        rdf_files += [os.path.join(root, f) for f in files if f.lower().endswith(rdf_exts)]
    return rdf_files


def _reorder_columns(df, categories):
    cols = list(df.columns)
    if "ExperimentId" in cols:
        cols.remove("ExperimentId")
    cols.sort(key=lambda c: (categories.get(c, ""), c))
    return df[["ExperimentId"] + cols] if "ExperimentId" in df.columns else df[cols]


def _save_all_outputs(df, arrow_table, meta_json_path, arrow_path):
    os.makedirs(os.path.dirname(meta_json_path), exist_ok=True)
    os.makedirs(os.path.dirname(arrow_path), exist_ok=True)

    with open(meta_json_path, "w", encoding="utf-8") as f:
        json.dump(df.dtypes.apply(str).to_dict(), f, indent=2)

    if meta_json_path.lower().endswith(".json"):
        meta_csv_path = meta_json_path.replace(".json", ".csv")
        df.dtypes.to_frame("dtype").reset_index().rename(columns={"index": "Variable"}).to_csv(meta_csv_path, index=False)
        print(f"✅ Metadata CSV: {meta_csv_path}")

    arrow_dir = os.path.dirname(arrow_path)
    stem = os.path.splitext(os.path.basename(arrow_path))[0]

    pq.write_table(arrow_table, arrow_path)
    df.to_json(os.path.join(arrow_dir, f"{stem}.json"), orient="records", indent=2)
    df.to_csv(os.path.join(arrow_dir, f"{stem}.csv"), index=False)
    df.to_excel(os.path.join(arrow_dir, f"{stem}.xlsx"), index=False)
    print(f"✅ Saved Arrow/CSV/Excel/JSON under: {arrow_dir}")


def _print_final_stats_and_preview(df, categories, rdf_files, src):
    print("\n📊 Final Stats")
    print(f"Files Processed: {len(rdf_files)} from {src}")
    print(f"Rows: {len(df)}, Columns: {len(df.columns)}")

    unique_cats = sorted(set(categories.values()) - {""})
    if unique_cats:
        print(f"Variable Categories: {len(unique_cats)} ({', '.join(unique_cats)})")
    else:
        print("No variable categories detected.")

    if not df.empty:
        print("🔎 First Row Preview:")
        print(df.iloc[0].to_dict())
    else:
        print("⚠️ No rows found.")
