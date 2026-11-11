**********************
Command-Line Interface
**********************

The ``FAIRLinked`` command exposes ontology, CSV conversion, and RDF Data Cube
workflows. Display the available commands with:

.. code-block:: console

   $ FAIRLinked --help

Use ``--help`` after any subcommand to see its current arguments:

.. code-block:: console

   $ FAIRLinked generate-template --help

Commands
========

.. list-table:: CLI commands
   :header-rows: 1
   :widths: 28 72

   * - Command
     - Purpose
   * - ``filter``
     - Find ontology terms associated with a domain, subdomain, or study stage.
   * - ``view-domains``
     - Display the domains and subdomains in MDS-Onto.
   * - ``dir-make``
     - Create a directory tree of domain and subdomain ontology files.
   * - ``add-terms``
     - Add terms to a local ontology file.
   * - ``term-search``
     - Search current MDS-Onto term labels using fuzzy matching.
   * - ``generate-template``
     - Generate a JSON-LD metadata template from a CSV file.
   * - ``serialize-data``
     - Serialize CSV records as JSON-LD files.
   * - ``deserialize-data``
     - Reconstruct tabular data from a directory of JSON-LD files.
   * - ``data-cube-run``
     - Start the interactive RDF Data Cube workflow.

Search MDS-Onto
===============

List ontology domains, then filter terms by subdomain:

.. code-block:: console

   $ FAIRLinked view-domains
   $ FAIRLinked filter -t SubDomain -q Chem-Rxn

Generate and serialize a CSV template
=====================================

Generate a template and matching logs:

.. code-block:: console

   $ FAIRLinked generate-template \
       --csv_path data.csv \
       --output_path metadata.jsonld \
       --log_path logs

After reviewing the template, serialize the data:

.. code-block:: console

   $ FAIRLinked serialize-data \
       --metadata_template metadata.jsonld \
       --csv_file data.csv \
       --row_key_cols Sample,Measurement \
       --orcid 0000-0001-2345-6789 \
       --output_folder records

Reconstruct tabular data
========================

.. code-block:: console

   $ FAIRLinked deserialize-data \
       --jsonld_directory records \
       --output_name reconstructed \
       --output_dir output

RDF Data Cube
=============

.. code-block:: console

   $ FAIRLinked data-cube-run

This command is interactive and requests the ontology, namespace, input data,
identifier columns, and output locations needed for the selected operation.
