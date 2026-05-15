from .MDS_DF.main import MatDatSciDf

def jsonld_directory_to_csv(input_dir, output_basename="merged_output", output_dir="outputs", orcid="unspecified"):
    """
    Refactored converter that utilizes MatDatSciDf factory methods to 
    reconstruct a FAIR dataset from a directory of RDF files.
    """
    
    # 1. Reconstruct the MatDatSciDf instance from the RDF directory.
    # This automatically handles:
    # - Recursive file walking and RDF parsing
    # - Data extraction (qudt:value) and Column mapping (skos:altLabel)
    # - Unit and Type consistency validation
    # - Generating a validation report in the input_dir
    mds_df = MatDatSciDf.from_rdf_dir(
        input_dir=input_dir,
        orcid=orcid,
        df_name=output_basename
    )

    if mds_df.df.empty:
        print(f"⚠️ No valid data reconstructed from {input_dir}. Check file extensions and SKOS labels.")
        return

    # 2. Save the reconstructed data and its semantic headers.
    # By setting 'metadata_in_output_df=True', save_mds_df will:
    # - Prepend the Type, Units, and Study Stage rows to the CSV
    # - Save 'clean' versions (data only) to Parquet and Arrow for storage
    # - Save the JSON-LD template and match logs
    mds_df.save_mds_df(
        output_dir=output_dir,
        metadata_in_output_df=True,
        formats=["csv", "parquet", "arrow"]
    )

    print(f"\n🚀 FAIR conversion complete. Files available in: {output_dir}")
