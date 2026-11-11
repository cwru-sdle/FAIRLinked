************************
Table Conversion API
************************

``FAIRLinked.RDFTableConversion`` provides the lower-level CSV-template
workflow. New Python projects will usually find :class:`MatDatSciDf
<FAIRLinked.RDFTableConversion.MDS_DF.main.MatDatSciDf>` more convenient.

Generate a metadata template
============================

.. automodule:: FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper

.. autofunction:: FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper.jsonld_template_generator

Serialize tabular data
======================

.. automodule:: FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler

.. autofunction:: FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler.extract_data_from_csv

Reconstruct tabular data
========================

.. automodule:: FAIRLinked.RDFTableConversion.jsonld_batch_converter

.. autofunction:: FAIRLinked.RDFTableConversion.jsonld_batch_converter.jsonld_directory_to_csv
