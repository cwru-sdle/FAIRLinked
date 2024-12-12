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
    should_save_csv
)
import os
from FAIRLinked.mds_ontology_analyzer import get_classification
from FAIRLinked.namespace_template_generator import generate_namespace_excel
from FAIRLinked.data_template_generator import generate_data_xlsx_template
from FAIRLinked.namespace_parser import parse_excel_to_namespace_map
from FAIRLinked.data_parser import read_excel_template
from FAIRLinked.rdf_transformer import convert_dataset_to_rdf
from FAIRLinked.rdf_to_df import parse_rdf_to_df
import traceback
from pprint import pprint

def main():
    print("Welcome to FAIRLinked 🚀")
    """
    Main entry point for the FAIRLinked data processing workflow.

    FAIRLinked converts rows of experimental data into Linked Data using the RDF Data Cube vocabulary, aligning with efforts to FAIRify data.

    This function orchestrates the entire workflow by determining the appropriate path based on the presence of an existing RDF data cube file. The workflow consists of the following paths:

    1. **Parse Existing Data Cube**: If an existing RDF data cube file is found, it parses the RDF back into a tabular format and optionally saves it as a CSV file.

    2. **Experiment Workflow**: If no existing data cube is found and the user chooses to run an experiment, it generates template Excel files for namespaces and data, optionally performing ontology analysis to map terms.

    3. **Standard Workflow**: If running the standard workflow, it processes existing namespace and data Excel files to generate RDF outputs, adhering to FAIR principles by producing Linked Data based on the RDF Data Cube.

    The function handles exceptions and provides informative messages to guide the user through the process of FAIRifying their data.
    """
    try:
        # Check for existing data cube file first
        has_file, file_path = has_existing_datacube_file()
        
        if has_file:
            # Get output paths for the parsed data
            output_folder = get_output_folder_path()
            variable_metadata_path = os.path.join(output_folder, "variable_metadata.json")
            arrow_output_path = os.path.join(output_folder, "dataset.parquet")
            
            # Parse existing RDF data cube back to tabular format
            table, metadata = parse_rdf_to_df(
                file_path=file_path,
                variable_metadata_json_path=variable_metadata_path,
                arrow_output_path=arrow_output_path
            )
            print("Successfully parsed RDF data cube to tabular format")
            
            # Simplified CSV saving option
            if should_save_csv():
                csv_path = os.path.join(output_folder, "output.csv")
                table.to_pandas().to_csv(csv_path, index=False)
                print(f"✅ DataFrame saved to {csv_path}")
            
            return
            
        # If no existing file, proceed with normal workflow
        is_experiment = check_if_running_experiment()
        if is_experiment:
            run_experiment_workflow()
        else:
            run_standard_workflow()
            
    except Exception as e:
        print(f"An error occurred in the main workflow: {e}")
        # Optionally, print more detailed error information
        # traceback.print_exc()
    finally:
        print("FAIRLinked exiting")

def run_experiment_workflow():
    """
    Generates namespace and data templates with optional ontology analysis for FAIRLinked.

    This function is part of FAIRLinked's workflow to assist users in preparing their experimental data for conversion into Linked Data. It performs the following steps:

    1. **Ontology Analysis (Optional)**:
        - Checks for the required ontology files.
        - Analyzes the lowest-level and combined MDS ontology files to classify and map terms to top-level categories.
        - Identifies any unmapped terms and displays warnings.

    2. **Generate Templates**:
        - Creates `namespace_template.xlsx` for defining namespaces.
        - Creates `data_template.xlsx` pre-populated with mapped terms if ontology analysis was performed.

    By generating these templates, FAIRLinked helps users structure their data in a way that facilitates conversion into the RDF Data Cube format, supporting data FAIRification efforts.

    Any exceptions encountered during the workflow are caught and an error message is displayed.
    """
    try:
        if has_all_ontology_files():
            # Get different ontology files
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
        
        # Generate templates with proper filenames
        generate_namespace_excel("./namespace_template.xlsx")
        generate_data_xlsx_template(mapped_terms, "./data_template.xlsx")
        
    except Exception as e:
        print(f"An error occurred in the experiment workflow: {e}")
        # traceback.print_exc()

def run_standard_workflow():
    """
    Processes namespace and data Excel files to generate RDF outputs using FAIRLinked.

    This function converts the user's experimental data into Linked Data based on the RDF Data Cube, aligning with FAIR principles. It executes the following steps:

    1. **Gather User Inputs**:
        - Retrieves the user's ORCID.
        - Obtains file paths for the namespace Excel file and the data Excel file.
        - Determines the output folder path.
        - Retrieves the dataset name from the user.

    2. **Parse Namespace and Data**:
        - Parses the namespace Excel file to create a namespace map.
        - Reads the data Excel template to extract variable metadata and data frames.

    3. **Convert to RDF**:
        - Uses the parsed data to convert the dataset into Linked Data in RDF Data Cube format.
        - Generates RDF outputs that include:
            - Dataset metadata
            - Variable mappings
            - Ontology linkages
            - FAIR compliance information

    4. **Save Outputs**:
        - Saves the RDF files in the specified output folder.

    By following this workflow, FAIRLinked assists users in FAIRifying their experimental data by converting it into interoperable Linked Data.

    The function handles exceptions and provides error messages to assist the user in troubleshooting any issues.
    """
    try:
        # Get user's ORCID
        orcid = get_orcid()
        # Get input namespace Excel file path
        namespace_excel_path = get_input_namespace_excel()
        # Get input data Excel file path
        data_excel_path = get_input_data_excel()
        # Get output folder path
        output_folder_path = get_output_folder_path()
        
        dataset_name = get_dataset_name()
        
        # Get namespace for the dataset
        # namespace_for_dataset = get_namespace_for_dataset()
        
        # Parse the namespace Excel to a namespace map
        namespace_map = parse_excel_to_namespace_map(namespace_excel_path)
        # Read the data Excel template
        variable_metadata, df = read_excel_template(data_excel_path)
        # Convert the dataset to RDF
        convert_dataset_to_rdf(
            df=df,
            variable_metadata=variable_metadata,
            namespace_map=namespace_map,
            user_chosen_prefix='mds',
            output_folder_path=output_folder_path,
            orcid=orcid,
            dataset_name=dataset_name,
            fixed_dimensions=None  # Adjust as needed
        )
    except Exception as e:
        print(f"An error occurred in the standard workflow: {e}")
        # traceback.print_exc()

if __name__ == "__main__":
    main()