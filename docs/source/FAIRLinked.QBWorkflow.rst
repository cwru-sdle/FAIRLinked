*****************
RDF Data Cube API
*****************

``FAIRLinked.QBWorkflow`` implements the interactive RDF Data Cube
serialization and deserialization workflow.

.. automodule:: FAIRLinked.QBWorkflow

Primary entry point
===================

The supported public entry point is the command-line workflow:

.. code-block:: console

   $ FAIRLinked data-cube-run

See :doc:`command-line` for usage. The implementation is divided across the
following modules:

.. automodule:: FAIRLinked.QBWorkflow.data_parser

``data_parser``
   Loads and validates tabular input.

.. automodule:: FAIRLinked.QBWorkflow.data_template_generator

``data_template_generator``
   Creates the workbook used to describe Data Cube observations.

.. automodule:: FAIRLinked.QBWorkflow.namespace_parser

``namespace_parser``
   Reads namespace definitions used during RDF generation.

.. automodule:: FAIRLinked.QBWorkflow.rdf_data_cube_workflow

``rdf_data_cube_workflow``
   Coordinates the interactive workflow.

.. automodule:: FAIRLinked.QBWorkflow.rdf_to_df

``rdf_to_df``
   Reconstructs tabular data from RDF Data Cube content.

.. automodule:: FAIRLinked.QBWorkflow.rdf_transformer

``rdf_transformer``
   Converts validated observations into RDF graphs.
