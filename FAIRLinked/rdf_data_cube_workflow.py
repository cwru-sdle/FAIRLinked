from FAIRLinked.input_handler import (
    check_if_running_experiment,
    get_domain,
    get_orcid,
    get_ontology_file,
    get_input_namespace_excel,
    get_input_data_excel,
    get_output_folder_path,
    get_dataset_name,
    get_namespace_for_dataset,
    has_all_ontology_files,
    has_existing_datacube_file,
    should_save_csv,
    choose_conversion_mode,
    check_ingestion,
    get_identifiers

)
import os
from FAIRLinked.mds_ontology_analyzer import get_classification
from FAIRLinked.namespace_template_generator import generate_namespace_excel
from FAIRLinked.data_template_generator import generate_data_xlsx_template
from FAIRLinked.namespace_parser import parse_excel_to_namespace_map
from FAIRLinked.data_parser import read_excel_template
from FAIRLinked.rdf_transformer import convert_dataset_to_rdf_with_mode
from FAIRLinked.rdf_to_df import parse_rdf_to_df
import traceback
from pprint import pprint

def rdf_data_cube_workflow_start():
    """
    Welcome to FAIRLinked 🚀

    The entry point for the FAIRLinked data processing workflow using RDF Data Cube.

    Steps Overview:
    1. Checks if an existing RDF data cube file/folder is present.
       - If yes, parse it back to tabular format (optionally saving CSV).
    2. If no existing data cube, prompts whether the user is running an experiment or not.
       - If experiment, generate namespace & data templates (with optional ontology analysis).
       - Otherwise, parse existing Excel files for namespaces & data, 
         then convert them to RDF in 'entire' or 'row-by-row' mode.
    """
    print("Welcome to FAIRLinked RDF Data Cube 🚀")

    try:
        # 1) Check if an existing RDF data cube is present
        has_file, file_path = has_existing_datacube_file()
        
        if has_file:
            # The user has an existing data cube (or folder). Let's parse it.
            parse_existing_datacube_workflow(file_path)
            return

        # 2) If no existing file, we do the normal workflow
        is_experiment = check_if_running_experiment()
        if is_experiment:
            run_experiment_workflow()
        else:
            ingestion_mode = check_ingestion()
            if ingestion_mode:
                run_ingestion_workflow()
            else:
                run_standard_workflow()

    except Exception as e:
        print(f"An error occurred in the main workflow: {e}")
        # traceback.print_exc()
    finally:
        print("FAIRLinked exiting")


def parse_existing_datacube_workflow(file_path: str):
    """
    If the user has an existing RDF data cube file or a directory of such files,
    parse it/them into a tabular format and optionally save as CSV.

    Args:
        file_path (str): Either a path to a single .ttl/.jsonld file 
                         or a directory containing multiple .ttl/.jsonld/.json-ld files.
    """
    try:
        # Ask user for an output folder (where we'll store the table + metadata)
        output_folder = get_output_folder_path()
        variable_metadata_path = os.path.join(output_folder, "variable_metadata.json")
        arrow_output_path = os.path.join(output_folder, "dataset.parquet")
        
        # Convert RDF data cube(s) => tabular format
        table, metadata = parse_rdf_to_df(
            file_path=file_path,
            variable_metadata_json_path=variable_metadata_path,
            arrow_output_path=arrow_output_path
        )
        print("Successfully parsed RDF data cube(s) to tabular format.")

        # Optionally prompt to save CSV
        if should_save_csv():
            csv_path = os.path.join(output_folder, "output.csv")
            table.to_pandas().to_csv(csv_path, index=False)
            print(f"✅ DataFrame saved to {csv_path}")
            
    except Exception as e:
        print(f"An error occurred while parsing the existing data cube: {e}")
        # traceback.print_exc()


def run_experiment_workflow():
    """
    Generates namespace and data templates with optional ontology analysis for FAIRLinked.

    Steps:
    1. Check if the user has local ontology files (lowest-level & combined).
    2. If found, run classification => map terms to categories.
    3. Generate 'namespace_template.xlsx' and 'data_template.xlsx', 
       optionally populating with mapped terms.
    """
    try:
        if has_all_ontology_files():
            # Prompt user for the two ontology files
            lowest_level_path = get_ontology_file("Lowest-level MDS ontology file")
            combined_path = get_ontology_file("Combined MDS ontology file")
            mapped_terms, unmapped_terms = get_classification(lowest_level_path, combined_path)
            
            if unmapped_terms:
                print("\nWarning: The following terms could not be mapped to top-level categories:")
                pprint(unmapped_terms, indent=2, width=80)
                print()
        else:
            print("\nGenerating default templates without ontology analysis...")
            mapped_terms = {}
        
        # Generate templates
        generate_namespace_excel("./namespace_template.xlsx")
        generate_data_xlsx_template(mapped_terms, "./data_template.xlsx")
        
    except Exception as e:
        print(f"An error occurred in the experiment workflow: {e}")
        # traceback.print_exc()


def run_standard_workflow():
    """
    Processes namespace and data Excel files to generate RDF outputs with FAIRLinked.

    Steps:
    1. Gather user inputs (ORCID, namespace/data Excel, output folder).
    2. Prompt for conversion mode (entire or row-by-row).
    3. If entire mode => ask for dataset name; if row-by-row => skip it.
    4. Parse the Excel templates => produce RDF using convert_dataset_to_rdf_with_mode.
    """
    try:
        orcid = get_orcid()
        namespace_excel_path = get_input_namespace_excel()
        data_excel_path = get_input_data_excel()
        output_folder_path = get_output_folder_path()

        # entire or row-by-row
        conversion_mode = choose_conversion_mode()

        if conversion_mode == "entire":
            dataset_name = get_dataset_name()
        else:
            # row-by-row doesn't require a single dataset name
            dataset_name = ""

        # Parse user-provided Excel for namespace map
        namespace_map = parse_excel_to_namespace_map(namespace_excel_path)

        # Read data Excel => variable_metadata + DataFrame
        variable_metadata, df = read_excel_template(data_excel_path)

        # Perform the conversion
        convert_dataset_to_rdf_with_mode(
            df=df,
            variable_metadata=variable_metadata,
            namespace_map=namespace_map,
            user_chosen_prefix='mds',
            output_folder_path=output_folder_path,
            orcid=orcid,
            dataset_name=dataset_name,
            fixed_dimensions=None,
            conversion_mode=conversion_mode
        )

    except Exception as e:
        print(f"An error occurred in the standard workflow: {e}")
        # traceback.print_exc()

def run_ingestion_workflow():

    """
        Processes namespace and data Excel files to generate RDF outputs with FAIRLinked.

    Steps:
    1. Gather user inputs (ORCID, namespace/data Excel, output folder).
    2. Prompt for conversion mode (entire or row-by-row).
    3. If entire mode => ask for dataset name; if row-by-row => skip it.
    4. Parse the Excel templates => produce RDF using convert_dataset_to_rdf_with_mode.
    """
    try:
        orcid = get_orcid()
        namespace_excel_path = get_input_namespace_excel()
        data_excel_path = get_input_data_excel()
        output_folder_path = get_output_folder_path()

        conversion_mode = "CRADLE"

        dataset_name = ""

        # Parse user-provided Excel for namespace map
        namespace_map = parse_excel_to_namespace_map(namespace_excel_path)

        # Read data Excel => variable_metadata + DataFrame
        variable_metadata, df = read_excel_template(data_excel_path)

        # Perform the conversion
        convert_dataset_to_rdf_with_mode(
            df=df,
            variable_metadata=variable_metadata,
            namespace_map=namespace_map,
            user_chosen_prefix='mds',
            output_folder_path=output_folder_path,
            orcid=orcid,
            dataset_name=dataset_name,
            fixed_dimensions=None,
            conversion_mode=conversion_mode
        )

    except Exception as e:
        print(f"An error occurred in the standard workflow: {e}")
        # traceback.print_exc()

        


if __name__ == "__main__":
    main()