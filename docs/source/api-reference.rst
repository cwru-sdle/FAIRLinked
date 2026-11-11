************
API Overview
************

This page provides a map of FAIRLinked's public interfaces followed by the
generated module reference. For implementation details, follow the source links
to the `FAIRLinked repository
<https://github.com/cwru-sdle/FAIRLinked/tree/main/FAIRLinked>`_.

.. toctree::
   :maxdepth: 3
   :caption: Module reference

   FAIRLinked

Top-level classes
=================

``FAIRLinked.MatDatSciDf``
--------------------------

Semantic wrapper around a pandas DataFrame. It manages column metadata,
ontology-backed relations, RDF serialization, and reconstruction.

Frequently used methods include:

* ``add_column_metadata`` and ``update_metadata``;
* ``add_entity`` and ``add_relations``;
* ``view_metadata`` and ``validate_metadata``;
* ``serialize_row`` and ``serialize_bulk``;
* ``from_rdf_dir`` and ``from_jsonld_list``; and
* ``save_mds_df``.

``FAIRLinked.AnalysisTracker``
------------------------------

Captures the inputs, outputs, environment, identity, and provenance associated
with one analysis run.

``FAIRLinked.AnalysisGroup``
----------------------------

Coordinates a collection of related analysis runs and produces group-level
reports and RDF provenance.

Package map
===========

``FAIRLinked.InterfaceMDS``
---------------------------

Tools for loading, browsing, searching, filtering, and extending MDS-Onto.
Primary modules include ``load_mds_ontology``, ``term_search_general``,
``domain_subdomain_viewer``, and ``add_ontology_term``.

``FAIRLinked.RDFTableConversion``
---------------------------------

CSV-template conversion functions. Primary entry points include
``jsonld_template_generator``, ``extract_data_from_csv``, and
``jsonld_directory_to_csv``.

``FAIRLinked.RDFTableConversion.MDS_DF``
----------------------------------------

Implementation package for ``MatDatSciDf``, metadata and relation managers,
``AnalysisTracker``, and ``AnalysisGroup``.

``FAIRLinked.QBWorkflow``
-------------------------

Interactive serialization and deserialization workflow based on the RDF Data
Cube vocabulary. The supported public CLI entry point is
``FAIRLinked data-cube-run``.

Command-line entry point
========================

The package installs ``FAIRLinked=FAIRLinked.cli.__main__:main``. See the
:doc:`command-line` for the supported commands.
