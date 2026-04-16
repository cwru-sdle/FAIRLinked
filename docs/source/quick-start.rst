================
Quick Start
================


This section provides a quick overview of RDFTableConversion.MDS_DF module, which is a refactoring of RDFTableConversion to help users FAIRify their data and analysis in an IDE.

Metadata management and analysis provenance with RDFTableConversion.MDS_DF
========================================================================

Serializing and deserializing with RDFTableConversion.MDS_DF
------------------------------------------------------------

The ``main.py`` module contains the **MatDatSciDf** class, which serves as an object that contains both the data and metadata associated with that data. It bridges tabular Pandas DataFrames and Linked Data (RDF) by maintaining synchronized metadata templates and ontological mappings.

Initialize the semantic wrapper. Setting ``metadata_rows=True`` instructs the class to ignore the first three rows. The method ``template_generator`` can be used to parse the metadata.

.. code-block:: python

   import pandas as pd
   from FAIRLinked import MatDatSciDf

   # Load the microindentation data
   raw_df = pd.read_csv("resources/worked-example-RDFTableConversion/microindentation/sa17455_00.csv")

   mds_df = MatDatSciDf(
       df=raw_df, 
       metadata_template={}, 
       orcid="0000-0001-2345-6789",
       df_name="PMMA_Hardness_Test",
       metadata_rows=True
   )

   template, matched, unmatched = mds_df.template_generator(skip_prompts=True)
   mds_df.metadata_template = template

To serialize the data, run ``serialize_row`` or ``serialize_bulk``:

.. code-block:: python

   row_graphs = mds_df.serialize_row(
       output_folder="resources/worked-example-RDFTableConversion.MDS_DF/individual_pmma_records",
       format='json-ld',
       row_key_cols=["Measurement", "Sample"], # Used to name files and generate IDs
       license="CC0-1.0"                      # Defaults to public domain
   )

   master_graph = mds_df.serialize_bulk(
       output_path="resources/worked-example-RDFTableConversion.MDS_DF/master_pmma_record.jsonld",
       format='json-ld',
       row_key_cols=["Measurement", "Sample"],
       license="MIT" 
   )

To turn serialized JSON-LDs back into an MDS_DF, run ``from_rdf_dir``:

.. code-block:: python

   import os
   from FAIRLinked import MatDatSciDf
   from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph

   # 1. Setup environment and load the reference ontology
   mds_graph = load_mds_ontology_graph()
   input_directory = "resources/worked-example-RDFTableConversion.MDS_DF/individual_pmma_records"

   # 3. Use the method to reconstruct.
   reconstructed_mds_df = MatDatSciDf.from_rdf_dir(
       input_dir=input_directory,
       orcid="0000-0001-2345-6789",
       df_name="Restored_PMMA_Study",
       ontology_graph=mds_graph
   )

.. list-table:: Serialization Methods
   :widths: 25 50 25
   :header-rows: 1

   * - Method
     - Description
     - Key Arguments
   * - ``template_generator``
     - Parses the isolated first 3 rows of the CSV to map Types, Units, and Stages.
     - ``skip_prompts``
   * - ``serialize_row``
     - Transforms each individual row of the DataFrame into its own RDF graph/file.
     - ``output_folder``, ``row_key_cols``, ``license``
   * - ``serialize_bulk``
     - Aggregates all row-level data into a single master graph file.
     - ``output_path``, ``row_key_cols``, ``license``
   * - ``from_rdf_dir``
     - Factory method that builds a new MatDatSciDf object from a directory of RDF files.
     - ``input_dir``, ``orcid``, ``ontology_graph``
   * - ``save_mds_df``
     - Exports the data to CSV (with semantic headers), Parquet, or Arrow.
     - ``output_dir``, ``metadata_in_output_df``

Metadata management
-------------------

Users can update their metadata template using ``update_metadata``, ``add_column_metadata``, or ``delete_column_metadata``.

.. code-block:: python

   mds_df.update_metadata(
       col_name="Hardness (GPa)", 
       field="definition", 
       value="Vickers hardness value measured on PMMA sample."
   )

   mds_df.add_column_metadata(
       col_name="YoungModulus",
       rdf_type="mds:YoungsModulus",
       unit="GigaPA",
       definition="Elastic modulus of the polymer.",
       study_stage="Result"
   )

.. list-table:: Metadata Management Methods
   :widths: 25 50 25
   :header-rows: 1

   * - Method
     - Description
     - Key Arguments
   * - ``update_metadata``
     - Updates a specific semantic field for an existing column entry.
     - ``col_name``, ``field``, ``value``
   * - ``add_column_metadata``
     - Manually defines semantic metadata for a new column or one found during an audit.
     - ``col_name``, ``rdf_type``, ``unit``, ``definition``
   * - ``delete_metadata``
     - Removes a column's entire semantic definition from the graph and template.
     - ``col_name``
   * - ``view_metadata``
     - Renders the current metadata template as a table or raw JSON-LD.
     - ``format`` ("table" or "json")
   * - ``validate_metadata``
     - Performs a two-way check to ensure DataFrame columns and metadata are aligned.
     - None

Data relations Management
-------------------------

.. code-block:: python

   micro_relations = {
       "is about": [
           ("Hardness (GPa)", "Sample"), 
           ("YoungModulus", "Sample")
       ],
       "mds:measuredBy": [
           ("Hardness (GPa)", "Indenter_ID"),
           ("Load (Newton)", "Indenter_ID")
       ]
   }

   mds_df.add_relations(micro_relations)
   mds_df.delete_relation("mds:measuredBy", ("Hardness (GPa)", "Indenter_ID"))

.. list-table:: Relation Management Methods
   :widths: 25 50 25
   :header-rows: 1

   * - Method
     - Description
     - Key Arguments
   * - ``add_relations``
     - Ingests a dictionary of links and validates them against the ontology.
     - ``data_relations`` (dict)
   * - ``delete_relation``
     - Removes a specific subject-object pair or an entire property group.
     - ``prop_key``, ``pair`` (optional)
   * - ``view_data_relations``
     - Generates a visual validation report of all current semantic links.
     - None
   * - ``get_relations``
     - Scans the active ontology to find available OWL properties.
     - None
   * - ``validate_data_relations``
     - Verifies predicates are ontologically valid.
     - None

Analysis Provenance
-------------------

**Batch Analysis with AnalysisGroup**

The ``AnalysisGroup`` class allows you to execute a function multiple times while capturing the provenance and data metadata for every iteration.

.. code-block:: python

   import numpy as np
   from sklearn.linear_model import LinearRegression
   from fairlinked import AnalysisGroup

   group = AnalysisGroup(
       proj_name="Lattice_Trend_Study", 
       home_path="path/to/data",
       orcid="0000-0001-2345-6789"
   )

   def perform_regression(X_data, y_data):
       model = LinearRegression().fit(np.array(X_data).reshape(-1, 1), y_data)
       return {
           "slope": model.coef_[0],
           "r_squared": model.score(np.array(X_data).reshape(-1, 1), y_data)
       }

   # Run the Batch
   for i in range(len(temperatures)):
       group.run_and_track(perform_regression, X_data=sub_x, y_data=sub_y)

   master_df = group.create_group_arg_df() 
   mds_obj = group.create_MatDatSciDf() 
   group.save_report()
   group.save_jsonld()


This next section provides example runs of the serialization and deserialization processes. All example files can be found in the GitHub repository of ``FAIRLinked`` under ``resources`` or can be directly accessed `here <https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/resources>`_. Command-line version of the functions below can be found `here <https://github.com/cwru-sdle/FAIRLinked/blob/main/resources/CLI_Examples.md>`_ and in the `change log <https://github.com/cwru-sdle/FAIRLinked/blob/main//CHANGELOG.md>`_.

Serializing and deserializing with RDFTableConversion from CSVs
==============================================================

To start serializing with FAIRLinked, we first make a template using ``jsonld_template_generator`` from ``FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper``. In your CSV, make sure to have some (possibly empty or partially filled) rows reserved for metadata about your variable.

.. note::
   Please make sure to follow the proper formatting guidelines for input CSV file:
   
   * Each column name should be the "common" or alternative name for this object.
   * The following three rows should be reserved for the **type**, **units**, and **study stage** in that order.
   * If values for these are not available, the space should be left blank.
   * Data for each sample can then begin on the 5th row.

Please see the following images for reference:

**Full Table**

.. image:: https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/fulltable.png
   :alt: Full Table



**Minimum Viable Data**

.. image:: https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/mintable.png
   :alt: Sparse Table

During the template generating process, the user may be prompted for data for different columns. When no units are detected, the user will be prompted for the type of unit, and then given a list of valid units to choose from.

.. image:: https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/kind.png
   :alt: Kind selection prompt

.. image:: https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/unit.png
   :alt: Unit selection prompt

When no study stage is detected, the user will similarly be given a list of study stages to choose from.

.. image:: https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/studystage.png
   :alt: Study stage prompt

The user will automatically be prompted for optional notes for each column.

**IN THIS FIRST EXAMPLE**, we will use the microindentation data of a PMMA, or Poly(methyl methacrylate), sample.

.. code-block:: python

    from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
    from FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper import jsonld_template_generator

    mds_graph = load_mds_ontology_graph()

    jsonld_template_generator(csv_path="resources/worked-example-RDFTableConversion/microindentation/sa17455_00.csv", 
                               ontology_graph=mds_graph, 
                               output_path="resources/worked-example-RDFTableConversion/microindentation/output_template.json", 
                               matched_log_path="resources/worked-example-RDFTableConversion/microindentation/microindentation_matched.txt", 
                               unmatched_log_path="resources/worked-example-RDFTableConversion/microindentation/microindentation_unmatched.txt",
                               skip_prompts=False)

The template is designed to capture the metadata associated with a variable, including units, study stage, row key, and variable definition. If the user does not wish to go through the prompts, set ``skip_prompts`` to ``True``.

After creating the template, run ``extract_data_from_csv`` using the template and CSV input to create JSON-LDs filled with data instances.

.. code-block:: python

    from FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler import extract_data_from_csv
    import json
    from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph

    mds_graph = load_mds_ontology_graph()

    with open("resources/worked-example-RDFTableConversion/microindentation/output_template.json", "r") as f:
        metadata_template = json.load(f) 

    prop_col_pair_dict = {"is about": [("PolymerGrade", "Sample"), 
                                        ("Hardness (GPa)", "Sample"),
                                        ("VickersHardness", "YoungModulus"),
                                        ("Load (Newton)", "Measurement"),
                                        ("ExposureStep","Measurement"), 
                                        ("ExposureType","Measurement"),
                                        ("MeasurementNumber","Measurement")]}

    extract_data_from_csv(metadata_template=metadata_template, 
                          csv_file="resources/worked-example-RDFTableConversion/microindentation/sa17455_00.csv",
                          orcid="0000-0001-2345-6789", 
                          output_folder="resources/worked-example-RDFTableConversion/microindentation/test_data_microindentation/output_microindentation",
                          row_key_cols=["Measurement", "Sample"],
                          id_cols=["Measurement", "Sample"],
                          prop_column_pair_dict=prop_col_pair_dict,
                          ontology_graph=mds_graph)

The arguments ``row_key_cols``, ``id_cols``, ``prop_column_pair_dict``, and ``ontology_graph`` are all optional. ``row_key_cols`` identifies columns used to create row keys, while ``id_cols`` specify identifiers of unique entities. ``prop_column_pair_dict`` defines the object or data properties used in the RDF graph.

To view the list of properties in MDS-Onto:

.. code-block:: python

    from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
    from FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler import generate_prop_metadata_dict

    mds_graph = load_mds_ontology_graph()
    view_all_props = generate_prop_metadata_dict(mds_graph)

    for key, value in view_all_props.items():
        print(f"{key}: {value}")

To deserialize your data, use ``jsonld_directory_to_csv``:

.. code-block:: python

    from FAIRLinked.RDFTableConversion.jsonld_batch_converter import jsonld_directory_to_csv

    jsonld_directory_to_csv(input_dir="resources/worked-example-RDFTableConversion/microindentation/test_data_microindentation/output_microindentation",
                            output_basename="sa17455_00_microindentation",
                            output_dir="resources/worked-example-RDFTableConversion/microindentation/test_data_microindentation/output_deserialize_microindentation")


Serializing and deserializing using RDF Data Cube with QBWorkflow
=================================================================

The RDF Data Cube Workflow is better run in ``bash``.

.. code-block:: bash

    $ FAIRLinked data-cube-run

This will start a series of prompts:

.. code-block:: text

    Welcome to FAIRLinked RDF Data Cube 🚀
    Do you have an existing RDF data cube dataset? (yes/no): no

Answer 'yes' to deserialize. If 'no', you will be asked if you are running an experiment:

.. code-block:: text

    Are you running an experiment now? (yes/no): yes

Once you've answered 'yes', provide the ontology files:

.. code-block:: text

    Do you have these ontology files (lowest-level, MDS combined)? (yes/no): yes
    Enter the path to the Lowest-level MDS ontology file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/Low-Level_Corrected.ttl
    Enter the path to the Combined MDS ontology file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/MDS_Onto_Corrected.ttl

This will generate a template at ``resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/data_template.xlsx``.

To serialize, start the workflow again and provide the following paths:

.. code-block:: text

    Enter ORC_ID: 0000-0001-2345-6789 
    Enter the path to the namespace Excel file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/namespace_template.xlsx
    Enter the path to the data Excel file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/mock_xrd_data.xlsx
    Enter the path to the output folder: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/output_serialize

Choose the conversion mode:

.. code-block:: text

    Do you want to convert the entire DataFrame as one dataset or row-by-row? (entire/row-by-row): row-by-row

Finally, select your row identifiers:

.. code-block:: text

    The following columns appear to be identifiers (contain 'id' in their name):
    Include column 'ExperimentId' in the row-based dataset naming? (yes/no): yes
    Include column 'DetectorWidth' in the row-based dataset naming? (yes/no): no
    Approved ID columns for naming: ['ExperimentId']
    Conversion completed under mode='row-by-row'.

To deserialize, answer 'yes' to the first question and provide the paths to your JSON-LD folder and output directory.







