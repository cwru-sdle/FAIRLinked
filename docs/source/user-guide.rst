**********
User Guide
**********

FAIRLinked provides several ways to add semantics to materials data. Start with
the DataFrame workflow unless you specifically need the RDF Data Cube model or
the older CSV-template interface.

Section Navigation
==================

.. toctree::
   :maxdepth: 2

   installation
   user-guide-pages/quickstart
   user-guide-pages/datatype-properties
   user-guide-pages/creating-entities

Choose a workflow
=================

.. list-table:: FAIRLinked workflows
   :header-rows: 1
   :widths: 24 44 32

   * - Workflow
     - Best for
     - Main interface
   * - Semantic DataFrame
     - Python and notebook users working with pandas data
     - ``MatDatSciDf``
   * - Ontology tools
     - Searching, filtering, or extending MDS-Onto
     - ``InterfaceMDS`` or the CLI
   * - CSV template
     - Existing pipelines based on metadata-header CSV files
     - ``RDFTableConversion``
   * - RDF Data Cube
     - Multidimensional datasets that must use the RDF Data Cube vocabulary
     - ``FAIRLinked data-cube-run``

Semantic DataFrame workflow
===========================

``MatDatSciDf`` keeps three parts of a dataset synchronized:

* a pandas DataFrame containing measurements;
* ontology-backed metadata describing columns and other entities; and
* semantic relationships between those entities.

The usual workflow is:

1. Create ``MatDatSciDf`` from a DataFrame.
2. Review automatically matched metadata with ``view_metadata``.
3. Add or update metadata and relationships.
4. Validate the result.
5. Serialize one graph per row or one graph for the full dataset.

Metadata
--------

Use ``update_metadata`` to refine the metadata created for a DataFrame column.
Use ``add_column_metadata`` only when a column has no metadata entry, and use
``add_entity`` for a semantic entity that is not a table column.

.. code-block:: python

   fair_data.update_metadata("Temperature", "type", "Temperature")
   fair_data.update_metadata("Temperature", "unit", "unit:DEG_C")
   fair_data.update_metadata(
       "Temperature",
       "definition",
       "Temperature during the measurement.",
   )
   fair_data.update_metadata("Temperature", "stage", "Result")

   fair_data.add_entity(
       name="Instrument",
       rdf_type="mds:Instrument",
       definition="Instrument used to collect the measurement.",
   )

Relations
---------

Relationships are expressed as a mapping from a property to one or more
``(subject, object)`` pairs:

.. code-block:: python

   fair_data.add_relations(
       {
           "is about": [("Hardness", "Sample")],
           "has agent": [("Measurement", "Instrument")],
       }
   )

Serialization
-------------

``serialize_bulk`` creates one graph for the full dataset.
``serialize_row`` creates a graph for each record. ``from_rdf_dir`` reconstructs
a semantic DataFrame from a directory of RDF files.

Ontology tools
==============

The ``FAIRLinked.InterfaceMDS`` package loads MDS-Onto and helps users explore
its terms.

.. code-block:: python

   from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
   from FAIRLinked.InterfaceMDS.term_search_general import term_search_general

   graph = load_mds_ontology_graph()
   term_search_general(
       mds_ontology_graph=graph,
       query_term="Chem-Rxn",
       search_types=["SubDomain"],
   )

The :doc:`command-line` provides equivalent commands for common ontology tasks.

CSV-template workflow
=====================

The lower-level ``RDFTableConversion`` interface supports CSV files whose first
three rows describe RDF type, unit, and study stage. It can generate a JSON-LD
metadata template, serialize rows, and reconstruct a CSV from JSON-LD files.

New projects should generally prefer ``MatDatSciDf`` because it exposes the
same concepts through a single object-oriented interface.

RDF Data Cube workflow
======================

Run the interactive workflow when your output must follow the `W3C RDF Data
Cube vocabulary <https://www.w3.org/TR/vocab-data-cube/>`_:

.. code-block:: console

   $ FAIRLinked data-cube-run

The command prompts for ontology, namespace, input data, identifiers, and output
locations. This workflow is more structured than the DataFrame workflow and is
intended for users already familiar with RDF Data Cube concepts.

Analysis provenance
===================

``AnalysisTracker`` captures one analysis run. ``AnalysisGroup`` coordinates
related runs and can create group-level reports and RDF provenance output. Both
classes are available from the top-level package:

.. code-block:: python

   from FAIRLinked import AnalysisGroup, AnalysisTracker

Continue to the :doc:`api-reference` for a compact map of the public package.
