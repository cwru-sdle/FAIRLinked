import re
from FAIRLinked.utility import validate_orcid_format
import os
from typing import Dict, Set, List, Tuple

def check_if_running_experiment() -> bool:
    """
    Prompts the user to answer whether they are currently running an experiment, accepting only 'yes' or 'no'.

    Args:
        None

    Returns:
        bool: Returns `True` if the user enters 'yes', otherwise `False`.

    Raises:
        ValueError: If the input is not 'yes' or 'no'.
    """

    while True:
        try:
            is_running_experiment = input("Are you running an experiment now? (yes/no): ").strip().lower()
            if is_running_experiment not in ["yes", "no"]:
                raise ValueError("Invalid input. Please enter 'yes' or 'no'.")
            return is_running_experiment == "yes"
        except ValueError as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

def get_domain(domains_hashset: Set[str]) -> str:
    """
    Prompts the user to select a domain from the available options (present in a hashset), ensuring proper handling of spaces and case.

    Args:
        domains_hashset (set): A set of available domain names.

    Returns:
        str: The selected domain name in lowercase.

    Raises:
        ValueError: If the input is not a valid number corresponding to a domain in the list.
    """

    while True:
        try:
            print("Available domains:")
            domains_list = list(domains_hashset)
            for i, domain in enumerate(domains_list, start=1):
                print(f"{i}. {domain}")
            selection = input("Enter the number corresponding to the domain: ").strip()
            if not selection.isdigit() or int(selection) < 1 or int(selection) > len(domains_list):
                raise ValueError("Invalid selection. Please enter a number from the list above.")
            return domains_list[int(selection) - 1].lower()
        except ValueError as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


def get_orcid() -> str:
    """
    Prompts the user to input an ORC_ID, ensuring that it conforms to the proper format by using a validation function.

    Args:
        None

    Returns:
        str: A valid ORC_ID string.

    Raises:
        ValueError: If the ORC_ID is empty or invalid.
    """

    while True:
        try:
            orcid = input("Enter ORC_ID: ").strip()
            if not orcid:
                raise ValueError("ORC_ID cannot be empty. Please enter a valid ORC_ID.")
            if not validate_orcid_format(orcid):
                raise ValueError("Invalid ORC_ID format. Please enter a valid ORC_ID.")
            return orcid
        except ValueError as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


def get_input_namespace_excel() -> str:
    """
    Prompts the user to enter the file path for a namespace Excel file and validates whether the file exists.

    Returns:
        str: The valid file path of the Excel file.

    Raises:
        FileNotFoundError: If the file path provided by the user does not exist.
    """

    while True:
        try:
            excel_file_path = input("Enter the path to the namespace Excel file: ").strip()
            if not os.path.isfile(excel_file_path):
                raise FileNotFoundError(f"The file '{excel_file_path}' does not exist. Please enter a valid file path.")
            # Optionally, check if the file has a valid Excel extension
            if not excel_file_path.lower().endswith(('.xlsx', '.xlsm', '.xltx', '.xltm')):
                raise ValueError("The file is not a valid Excel file. Please provide a file with an '.xlsx', '.xlsm', '.xltx', or '.xltm' extension.")
            return excel_file_path
        except (FileNotFoundError, ValueError) as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

def get_input_data_excel() -> str:
    """
    Prompts the user to enter the file path for a data Excel file and validates whether the file exists.

    Returns:
        str: The valid file path of the Excel file.

    Raises:
        FileNotFoundError: If the file path provided by the user does not exist.
    """

    while True:
        try:
            excel_file_path = input("Enter the path to the data Excel file: ").strip()
            if not os.path.isfile(excel_file_path):
                raise FileNotFoundError(f"The file '{excel_file_path}' does not exist. Please enter a valid file path.")
            # Optionally, check if the file has a valid Excel extension
            if not excel_file_path.lower().endswith(('.xlsx', '.xlsm', '.xltx', '.xltm')):
                raise ValueError("The file is not a valid Excel file. Please provide a file with an '.xlsx', '.xlsm', '.xltx', or '.xltm' extension.")
            return excel_file_path
        except (FileNotFoundError, ValueError) as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")



def get_output_folder_path() -> str:
    """
    Prompts the user to provide an output folder path, and creates the folder if it does not exist.

    Args:
        None

    Returns:
        str: The valid path to the output folder.

    Raises:
        NotADirectoryError: If the path provided is not a valid directory.
    """

    while True:
        try:
            folder_path = input("Enter the path to the output folder: ").strip()
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"The folder {folder_path} has been created.")
            elif not os.path.isdir(folder_path):
                raise NotADirectoryError(f"The path {folder_path} is not a directory. Please enter a valid directory path.")
            return folder_path
        except NotADirectoryError as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            
def get_namespace_for_dataset(namespace_map: Dict[str, str]) -> str:
    """
    Prompts the user to select a namespace for their dataset, excluding predefined standard vocabulary namespaces.

    Args:
        namespace_map (dict): A dictionary where the keys are namespace prefixes and the values are corresponding base URIs.

    Returns:
        str: The selected namespace prefix.

    Raises:
        ValueError: If the user selects a number outside the valid range.
    """

    standard_vocab_namespaces = [
        "rdf", "rdfs", "owl", "xsd", "skos", "void", "dct", "foaf", "org",
        "admingeo", "interval", "qb", "sdmx-concept", "sdmx-dimension", "sdmx-attribute",
        "sdmx-measure", "sdmx-metadata", "sdmx-code", "sdmx-subject", "qudt", "ex-geo"
    ]
    filtered_namespace_map = {k: v for k, v in namespace_map.items() if k not in standard_vocab_namespaces}

    while True:
        try:
            print("Available namespaces:")
            namespaces_list = list(filtered_namespace_map.keys())
            for i, namespace in enumerate(namespaces_list, start=1):
                print(f"{i}. {namespace} ({filtered_namespace_map[namespace]})")
            selection = input("Enter the number corresponding to the namespace you want to use: ").strip()
            if not selection.isdigit() or int(selection) < 1 or int(selection) > len(namespaces_list):
                raise ValueError("Invalid selection. Please enter a number from the list above.")
            return namespaces_list[int(selection) - 1]
        except ValueError as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

def get_ontology_file(prompt_message: str) -> str:
    """
    Prompts the user to enter the file path for an ontology file and validates whether it exists
    and has the correct extension (.ttl).

    Args:
        prompt_message (str): Custom prompt message to specify which ontology file is being requested.

    Returns:
        str: The valid file path of the ontology file.

    Raises:
        FileNotFoundError: If the file path provided does not exist.
        ValueError: If the file does not have a .ttl extension.
    """
    while True:
        try:
            file_path = input(f"Enter the path to the {prompt_message}: ").strip()
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"The file '{file_path}' does not exist. Please enter a valid file path.")
            if not file_path.lower().endswith('.ttl'):
                raise ValueError("The file must be a Turtle (.ttl) ontology file.")
            return file_path
        except (FileNotFoundError, ValueError) as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

def get_dataset_name() -> str:
    """
    Prompts the user to enter a name for their dataset. Only allows letters and underscores.
    Offers 'SampleDataset' as a fallback option if invalid input is provided.

    Returns:
        str: A valid dataset name containing only letters, numbers, and underscores.
    """
    while True:
        try:
            name = input("Enter a name for your dataset (letters, numbers, and underscores only): ").strip()
            if re.match(r'^[A-Za-z0-9_]+$', name):
                return name
            
            use_default = input("Invalid name. Would you like to use 'SampleDataset' instead? (yes/no): ").strip().lower()
            if use_default == 'yes':
                return 'SampleDataset'
            print("Please try again with only letters, numbers, and underscores.")
            
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

def has_all_ontology_files() -> bool:
    """
    Prompts the user to confirm availability of all required ontology files.

    Args:
        None

    Returns:
        bool: True if the user indicates they have both required ontology files 
              (lowest-level, combined), False otherwise.

    Raises:
        ValueError: If the user provides an answer other than 'yes' or 'no'.
    """
    while True:
        try:
            response = input("Do you have these ontology files (lowest-level, MDS combined)? (yes/no): ").strip().lower()
            if response not in ["yes", "no"]:
                raise ValueError("Please answer 'yes' or 'no'")
            return response == "yes"
        except ValueError as e:
            print(e)

def has_existing_datacube_file() -> Tuple[bool, str]:
    """
    Prompts the user to specify if they have an existing RDF data cube dataset,
    which may be either:
      - A single file (.ttl, .jsonld)
      - A directory that contains .ttl/.jsonld files
    Returns:
        (False, "") if user says 'no',
        (True, path) if user says 'yes' and provides a valid path that exists
        (whether it's a file or directory).
    """
    while True:
        try:
            has_file = input("Do you have an existing RDF data cube dataset? (yes/no): ").strip().lower()
            if has_file not in ["yes", "no"]:
                raise ValueError("Please answer 'yes' or 'no'")

            if has_file == "no":
                return False, ""

            # Here, the user said "yes."
            file_path = input(
                "Enter the path to your RDF data cube file/folder (can be .ttl/.jsonld or a directory): "
            ).strip()

            # We now allow either a file or a directory, but it must exist:
            if not os.path.exists(file_path):
                print(f"The path '{file_path}' does not exist. Please enter a valid path.")
                continue

            # If it exists (directory or file), we accept it
            return True, file_path

        except ValueError as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


def should_save_csv() -> bool:
    """
    Prompts the user whether they want to save the DataFrame as CSV.

    Returns:
        bool: True if user wants to save CSV, False otherwise
    """
    while True:
        try:
            save_csv = input("Do you want to save the DataFrame to a CSV file? (yes/no): ").strip().lower()
            if save_csv not in ["yes", "no"]:
                raise ValueError("Please answer 'yes' or 'no'")
            return save_csv == "yes"
            
        except ValueError as e:
            print(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

def choose_conversion_mode() -> str:
    """
    Prompts the user to decide whether to convert the entire DataFrame as one dataset
    or each row as an individual dataset for RDF conversion.

    Returns:
        str: 'entire' or 'row-by-row'
    """
    while True:
        try:
            mode = input("Do you want to convert the entire DataFrame as one dataset or row-by-row? (entire/row-by-row): ").strip().lower()
            if mode not in ["entire", "row-by-row"]:
                raise ValueError("Invalid choice. Please enter 'entire' or 'row-by-row'.")
            return mode
        except ValueError as e:
            print(e)
            
def get_approved_id_columns(candidate_id_columns: List[str], mode: str) -> List[str]:
    """
    Description:
        Given a list of candidate ID columns (those that contain 'id' in their name),
        prompts the user for each column whether they want it included in the dataset 
        naming. The prompt text changes depending on whether we are in 'row-by-row' 
        mode or 'entire' mode.

    Algorithm:
        1) If there are no candidate_id_columns, print a message and return empty list.
        2) Print each candidate column to the user, 
           asking if it should be included in naming.
           - If mode == 'row-by-row', mention "row-based dataset naming."
           - If mode == 'entire', mention "slice naming for entire dataset."
        3) Collect approved columns in a list.
        4) Return that list.

    Args:
        candidate_id_columns (List[str]): Columns that appear to be ID-like (contain "id" or "ID").
        mode (str): 'row-by-row' or 'entire'—used to tailor the prompt text.

    Returns:
        List[str]: Subset of candidate_id_columns that the user approves for naming.
    """
    approved_columns = []
    if not candidate_id_columns:
        print("No ID-like columns found. Naming will only use ORCID + timestamp.")
        return approved_columns

    print("\nThe following columns appear to be identifiers (contain 'id' in their name):")
    for col in candidate_id_columns:
        while True:
            try:
                if mode == "row-by-row":
                    question_text = f"Include column '{col}' in the row-based dataset naming? (yes/no): "
                else:
                    # entire
                    question_text = f"Include column '{col}' in the slice naming for the entire dataset? (yes/no): "

                answer = input(question_text).strip().lower()
                if answer not in ["yes", "no"]:
                    raise ValueError("Please answer 'yes' or 'no'.")
                if answer == 'yes':
                    approved_columns.append(col)
                break
            except ValueError as e:
                print(e)

    if not approved_columns:
        print("No columns were approved. Naming will be ORCID + timestamp only.")
    else:
        print(f"Approved ID columns for naming: {approved_columns}")

    return approved_columns

def check_ingestion() -> str:
    """
    Prompts the user whether data is for ingesting into CRADLE.

    Returns:
        bool: True if data is for ingesting into CRADLE, False otherwise
    """
    while True:
        try:
            ingestion_mode = input("Do you have data for CRADLE ingestion? (yes/no): ").strip().lower()
            if ingestion_mode not in ["yes","no"]:
                raise ValueError("Please answer 'yes' or 'no'.")
            else:
                return ingestion_mode == "yes"
        except ValueError as e:
            print(e)


def get_identifiers(approved_id_cols) -> dict:
    """
    Prompt the user to enter identifiers
    """
    id_dict = {}
    fields = ["SampleId", "ToolId", "RecipeId", "ExposureId", "StepId"]

    approved_id_cols_lower = {col.lower() for col in approved_id_cols}
    
    missing_ids = [id for id in fields if id.lower() not in approved_id_cols_lower]
    print(f"\nMissing Identifiers: {missing_ids}")

    for missing_id in missing_ids:
        while True:
            try:
                user_input = input(f"Enter {missing_id} (Leave blank if not available): ").strip()
                check_valid_id(user_input)
                id_dict[missing_id] = user_input
                break  # Move to the next missing_id if input is valid
            except ValueError as e:
                print(f"Error: {e}. Please enter a valid {missing_id}.")

    print(f"\nCollected Identifiers: {id_dict}")
    return id_dict

def check_valid_id(user_input):
    if user_input and not re.fullmatch(r"[A-Za-z0-9]+", user_input):
        raise ValueError("Input must contain only letters and numbers (no spaces or special characters).")














