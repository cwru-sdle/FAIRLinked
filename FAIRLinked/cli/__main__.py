
import argparse
import ast

# Import your subpackage entry functions
from FAIRLinked.InterfaceMDS import add_term_to_ontology, term_search_general, domain_subdomain_viewer, domain_subdomain_directory, domain_subdomain_dir_interface, fuzzy_search_interface, filter_interface
from FAIRLinked.RDFTableConversion import extract_data_from_csv, extract_from_folder, jsonld_directory_to_csv, jsonld_temp_gen_interface, extract_data_from_csv_interface
from FAIRLinked.QBWorkflow import rdf_data_cube_workflow_start


def comma_separated_list(value):
    return [v.strip() for v in value.split(",") if v.strip()]

def main():
    parser = argparse.ArgumentParser(prog="FAIRLinked", description="FAIRLinked CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --------------------
    # InterfaceMDS commands
    # --------------------

    # Term search and filter command
    filter_parser = subparsers.add_parser("filter", help="Get terms associated with a certain Domain, Subdomain, or Study Stage",
        description="""
        Term search using Domain, SubDomain, or Study Stage. For complete list of Domains and SubDomains, 
        run the following commands in bash:

        FAIRLinked view-domains
        FAIRLinked dir-make. 

        The current list of Study Stages includes: 
        Synthesis, 
        Formulation, 
        Materials Processing, 
        Sample, 
        Tool, 
        Recipe, 
        Result,
        Analysis,
        Modelling.

        For more details about MDS Study Stages, please visit https://cwrusdle.bitbucket.io/ for more information.

        """,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    filter_parser.add_argument("-op", "--ontology_path", default="default", help="Path to ontology file")
    filter_parser.add_argument("-q", "--query_term", help="Term to search for")
    filter_parser.add_argument(
        "-t", "--search_types", nargs="+",
        choices=["Domain", "SubDomain", "Study Stage"], required=True, help="Specifies the search criteria"
    )
    filter_parser.add_argument("-te", "--ttl_extr",
        choices=["T", "F"],
        default="F",
        help="Specifies whether user wants to save search results. Enter T or F"
    )
    filter_parser.add_argument("-tp", "--ttl_path", 
        help="If user wants to save search results, provide path to save file. Append file name at the end of the path"
        )
    filter_parser.set_defaults(func=filter_interface)

    # Domain and subdomain viewer
    viewer_parser = subparsers.add_parser(
        "view-domains",
        help="Display unique Domains and SubDomains"
    )
    viewer_parser.set_defaults(func=lambda args: domain_subdomain_viewer())

    # Domain and subdomain directory viewer
    domain_dir_view = subparsers.add_parser(
        "dir-make",
        help="View and make directory tree of turtle files based on domains and subdomains"
    )
    domain_dir_view.set_defaults(func=lambda args: domain_subdomain_dir_interface())

    # Add term to ontology
    onto_add_parser = subparsers.add_parser(
        "add-terms",
        help="Add new terms to an existing ontology file"
    )
    onto_add_parser.add_argument("-op","--onto_file_path", help="Path to ontology file")
    onto_add_parser.set_defaults(func=lambda args: add_term_to_ontology(args.onto_file_path))

    # Fuzzy string search
    string_search_parser = subparsers.add_parser(
        "term-search",
        help="Search for terms by matching term labels"
    )
    string_search_parser.set_defaults(func=lambda args: fuzzy_search_interface())


    # --------------------
    # RDFTableConversion commands
    # --------------------
    
    # Generate JSON-LD templates
    jsonld_temp_gen_parser = subparsers.add_parser(
        "generate-template",
        help="Generate a JSON-LD template based on a CSV",
        description=
        """
        Generate a template that will allow the users to fill in metadata about columns in their dataframe, including units,
        definitions, and explanatory notes. For column labels that can be matched to a term in MDS-Onto, definition will be filled out.
        """
    )
    jsonld_temp_gen_parser.add_argument("-cp", "--csv_path", required=True, help="Path to CSV file")
    jsonld_temp_gen_parser.add_argument("-op", "--ontology_path", help="Path to ontology. To get official MDS-Onto choose 'default'")
    jsonld_temp_gen_parser.add_argument("-out", "--output_path", required=True, help="Path to output JSON-LD file")
    jsonld_temp_gen_parser.add_argument("-lp", "--log_path", required=True, help="Path to store files that log labels that could/couldn't be matched to a term in MDS-Onto")
    jsonld_temp_gen_parser.set_defaults(func=jsonld_temp_gen_interface)

    # Create directory of JSON-LDs from CSV
    data_extract_parser = subparsers.add_parser(
        "serialize-data",
        help="Create a directory of JSON-LDs from a single CSV"
    )
    data_extract_parser.add_argument("-mdt",
        "--metadata_template",
        required=True,
        help="Metadata template (path to JSON file if using CLI)"
    )
    data_extract_parser.add_argument("-cf",
        "--csv_file",
        required=True,
        help="Path to the CSV file containing the data"
    )
    data_extract_parser.add_argument("-rkc",
        "--row_key_cols",
        type=comma_separated_list,
        help="Choose column used to uniquely identify rows, default will generate keys based off all columns"
    )
    data_extract_parser.add_argument("-ic",
        "--id-cols",
        type = comma_separated_list,
        help = "Choose columns used to uniquely identify a specific entity, such as a sample, a sample set, tool, etc..."
    )
    data_extract_parser.add_argument("-orc",
        "--orcid",
        required=True,
        help="ORCID identifier of the researcher"
    )
    data_extract_parser.add_argument("-of",
        "--output_folder",
        required=True,
        help="Directory where JSON-LD files will be saved"
    )
    data_extract_parser.add_argument("-pc",
        "--prop_col",
        type=ast.literal_eval,
        help="""
        Python dictionary literal: enter a dictionary with keys as labels of OWL properties and values are lists of 2-tuples.
        Enter a string in the form of "{"rel_1": [(col_1, col_2), (col_3, col_4)], "rel_2":[(col_3, col_6)]}"
        """
    )
    data_extract_parser.add_argument("-op",
        "--ontology_path",
        default="default",
        help="Path to ontology. Must be provided if 'prop_col' is provided. To get official MDS-Onto choose 'default'"
        )
    data_extract_parser.add_argument("-base",
        "--base_uri",
        default="https://cwrusdle.bitbucket.io/mds/",
        help="Base URI used to construct subject and object URIs"
    )

    data_extract_parser.add_argument("-l",
        "--license",
        default =None,
        help="License used, find valid licenses at https://spdx.org/licenses/"
    )
    data_extract_parser.set_defaults(func=extract_data_from_csv_interface)

    # Deserialize a directory of JSON-LDs back into a CSV
    deserializer_parser = subparsers.add_parser(
        "deserialize-data",
        help="Deserialize a directory of JSON-LDs back to a CSV"
    )
    deserializer_parser.add_argument("-jd",
        "--jsonld_directory",
        required=True,
        help="Directory containing JSON-LD files"
    )
    deserializer_parser.add_argument("-on", 
        "--output_name",
        required=True,
        help="Base name of output files"
    )
    deserializer_parser.add_argument("-od",
        "--output_dir",
        required=True,
        help="Path to directory to save the outputs"
    )
    deserializer_parser.add_argument("-rk",
        "--primary_row_key",
        required=False,
        help="Optional value for primary key, values will be generated if not specified"
    )
    deserializer_parser.set_defaults(func=lambda args: jsonld_directory_to_csv(args.jsonld_directory, args.output_name, args.output_dir,args.primary_row_key))

    # --------------------
    # QBWorkflow commands
    # --------------------
    data_cube_workflow_parser = subparsers.add_parser(
        "data-cube-run",
        help="Start RDF Data Cube Workflow",
        description="""
        RDF Data Cube is a comprehensive FAIRification workflow designed for users familiar with the 
        RDF Data Cube (see https://www.w3.org/TR/vocab-data-cube/) vocabulary. This workflow supports the creation of 
        richly structured, multidimensional datasets that adhere to linked data best practices and can be easily queried, 
        combined, and analyzed.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    data_cube_workflow_parser.set_defaults(func=lambda args: rdf_data_cube_workflow_start())


    # --------------------
    # Dispatch
    # --------------------
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
