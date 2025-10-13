Quick Start
==========


***************
General Usage
***************

The tool is invoked from the command line using the main script name (assumed here to be ``FAIRLinked``), followed by a command and its specific arguments.

.. code-block:: bash

   FAIRLinked [COMMAND] [OPTIONS]

To see the help message for any command, use the ``-h`` or ``--help`` flag.

.. code-block:: bash

   FAIRLinked filter -h
   FAIRLinked generate-template --help

----

***********************
InterfaceMDS Commands
***********************

These commands are used for interacting with the Materials Data Schema (MDS) ontology.

filter
======

Get terms associated with a certain Domain, Subdomain, or Study Stage.

**Description:**
Term search using Domain, SubDomain, or Study Stage. For a complete list of Domains and SubDomains, run ``FAIRLinked view-domains`` and ``FAIRLinked dir-make``. The current list of Study Stages includes: Synthesis, Formulation, Materials Processing, Sample, Tool, Recipe, Result, Analysis, Modelling. For more details, please visit `the SDLE homepage <https://cwrusdle.bitbucket.io/>`_.

**Usage:**

.. code-block:: bash

   FAIRLinked filter -t <SEARCH_TYPES> -q <QUERY_TERM> [OPTIONS]

**Arguments:**

* ``-t, --search_types``: (**Required**) Specifies the search criteria. Choices: ``"Domain"``, ``"SubDomain"``, ``"Study Stage"``. You can provide one or more.
* ``-q, --query_term``: (**Required**) Enter the domain, subdomain, or study stage.
* ``-op, --ontology_path``: Path to the ontology file. Defaults to ``"default"``.
* ``-te, --ttl_extr``: Specifies whether to save search results. Choices: ``"T"`` or ``"F"``. Defaults to ``"F"``.
* ``-tp, --ttl_path``: If saving results (``-te T``), provide the full path and filename for the output.

**Example:**
Search for the term "Chem-rxn" within the "Domain" search type.

.. code-block:: bash

   FAIRLinked filter -t "Domain" -q "Chem-rxn"

Search for terms in the "Sample" Study Stage and save the results to a file.

.. code-block:: bash

   FAIRLinked filter -t "Study Stage" -q "Sample" -te "T" -tp "/path/to/save/sample_terms.ttl"

view-domains
============

Display unique Domains and SubDomains from the ontology.

**Usage:**

.. code-block:: bash

   FAIRLinked view-domains

dir-make
========

View and make a directory tree of turtle files based on domains and subdomains.

**Usage:**

.. code-block:: bash

   FAIRLinked dir-make

add-terms
=========

Add new terms to an existing ontology file. This command launches an interactive session to guide you through the process.

**Usage:**

.. code-block:: bash

   FAIRLinked add-terms -op <PATH_TO_ONTOLOGY>

**Arguments:**

* ``-op, --onto_file_path``: Path to the ontology file you want to modify.

**Example:**

.. code-block:: bash

   FAIRLinked add-terms -op "/path/to/my_ontology.ttl"

term-search
===========
Search for terms by matching term labels using a fuzzy search algorithm. This command is interactive.

**Usage:**

.. code-block:: bash

   FAIRLinked term-search

----

****************************
RDFTableConversion Commands
****************************

These commands facilitate the conversion of tabular data (CSV) to and from RDF (JSON-LD format).

generate-template
=================
Generate a JSON-LD template based on a CSV file.

**Description:**
This command generates a template that allows users to fill in metadata about columns in their dataframe, including units, definitions, and explanatory notes. For column labels that can be matched to a term in MDS-Onto, the definition will be pre-filled.

**Usage:**

.. code-block:: bash

   FAIRLinked generate-template -cp <CSV_PATH> -out <OUTPUT_PATH> -lp <LOG_PATH> [OPTIONS]

**Arguments:**

* ``-cp, --csv_path``: (**Required**) Path to the input CSV file.
* ``-out, --output_path``: (**Required**) Path to save the output JSON-LD template file.
* ``-lp, --log_path``: (**Required**) Path to a directory to store log files detailing which labels were matched.
* ``-op, --ontology_path``: Path to the ontology file. Use ``"default"`` for the official MDS-Onto.

**Example:**

.. code-block:: bash

   FAIRLinked generate-template -cp "./data/experiments.csv" -out "./metadata/template.json" -lp "./logs/" -op "default"

serialize-data
==============
Create a directory of JSON-LD files from a single CSV file and a metadata template.

**Usage:**

.. code-block:: bash

   FAIRLinked serialize-data -mdt <TEMPLATE_PATH> -cf <CSV_PATH> -rkc <ROW_KEY_COLS> -orc <ORCID> -of <OUTPUT_FOLDER> [OPTIONS]

**Arguments:**

* ``-mdt, --metadata_template``: (**Required**) Path to the completed JSON-LD metadata template file.
* ``-cf, --csv_file``: (**Required**) Path to the CSV file containing the data.
* ``-rkc, --row_key_cols``: (**Required**) Comma-separated list of column names that uniquely identify rows (e.g., ``"col1,col2,col3"``). No spaces between names.
* ``-orc, --orcid``: (**Required**) ORCID identifier of the researcher (e.g., ``"0000-0001-2345-6789"``).
* ``-of, --output_folder``: (**Required**) Directory where the generated JSON-LD files will be saved.
* ``-pc, --prop_col``: A Python dictionary literal (as a string) defining relationships between columns.
* ``-op, --ontology_path``: Path to the ontology file. Required if ``-pc`` is provided.
* ``-base, --base_uri``: Base URI used to construct subject and object URIs. Defaults to ``https://cwrusdle.bitbucket.io/mds/``.

**Example:**

.. code-block:: bash

   FAIRLinked serialize-data \
       -mdt "./metadata/template.json" \
       -cf "./data/experiments.csv" \
       -rkc "SampleID,RunNumber" \
       -orc "0000-0001-2345-6789" \
       -of "./output/jsonld_files/"

**Example with ``-pc`` argument:**
This example states that the value in the ``ProcessStep`` column is related to the value in the ``MaterialID`` column via the ``hasInput`` property.

.. code-block:: bash

   FAIRLinked serialize-data \
       -mdt "./metadata/template.json" \
       -cf "./data/experiments.csv" \
       -rkc "SampleID" \
       -orc "0000-0001-2345-6789" \
       -of "./output/jsonld_files/" \
       -op "default" \
       -pc '{"hasInput": [("ProcessStep", "MaterialID")], "hasOutput":[("ProcessStep", "OutputMaterialID")]}'

deserialize-data
================
Deserialize a directory of JSON-LD files back into a CSV file.

**Usage:**

.. code-block:: bash

   FAIRLinked deserialize-data -jd <JSONLD_DIRECTORY> -on <OUTPUT_NAME> -od <OUTPUT_DIR>

**Arguments:**

* ``-jd, --jsonld_directory``: (**Required**) Directory containing the JSON-LD files.
* ``-on, --output_name``: (**Required**) The base name for the output files (e.g., "my_deserialized_data").
* ``-od, --output_dir``: (**Required**) Path to the directory where the output CSV will be saved.

**Example:**

.. code-block:: bash

   FAIRLinked deserialize-data \
       -jd "./output/jsonld_files/" \
       -on "reconstructed_data" \
       -od "./data/reconstructed/"

----

*********************
QBWorkflow Commands
*********************

Commands related to the RDF Data Cube workflow.

data-cube-run
=============

Start the RDF Data Cube Workflow.

**Description:**
The RDF Data Cube is a comprehensive FAIRification workflow designed for users familiar with the `RDF Data Cube vocabulary <https://www.w3.org/TR/vocab-data-cube/>`_. This workflow supports the creation of richly structured, multidimensional datasets that adhere to linked data best practices and can be easily queried, combined, and analyzed. This command will launch an interactive workflow.

**Usage:**

.. code-block:: bash

   FAIRLinked data-cube-run








