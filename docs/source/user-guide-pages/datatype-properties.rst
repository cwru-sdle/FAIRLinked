*************************
Using Datatype Properties
*************************

FAIRLinked supports datatype properties using the same relationship mapping
used for object properties. A datatype property connects an entity to a literal
value such as a number, date, or text string.

Map literal values
==================

To map a literal value:

1. Store the values in a dataset column.
2. Use a datatype property to connect an entity or object column to the literal
   value column.

The relationship has the form:

**Object -> datatype property -> column containing literal values**

The supplied example associates the values in ``Latitude`` and ``Longitude``
with the object represented by the ``Centroid`` column.

Define external datatype properties
===================================

Latitude and longitude are not yet defined as datatype properties in MDS-Onto,
so the example registers the W3C Basic Geo properties in the active ontology:

.. code-block:: python

   from rdflib import Namespace
   from rdflib.namespace import OWL, RDF

   GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

   mds_df.ontology.add((GEO.lat, RDF.type, OWL.DatatypeProperty))
   mds_df.ontology.add((GEO.long, RDF.type, OWL.DatatypeProperty))

Map the literal columns
=======================

Use the full property IRIs to connect ``Centroid`` to its coordinate values:

.. code-block:: python

   mds_df.add_relations(
       {
           "http://www.w3.org/2003/01/geo/wgs84_pos#lat": [
               ("Centroid", "Latitude")
           ],
           "http://www.w3.org/2003/01/geo/wgs84_pos#long": [
               ("Centroid", "Longitude")
           ],
       }
   )

The full example also maps object properties between the primary ``Geohash``
and its eight neighboring geohash columns. It then validates the relations and
serializes one JSON-LD file per row.

.. note::

   The example CSV contains three blank metadata rows, so the script passes
   ``metadata_rows=True``.

Download the exact examples
===========================

These are unchanged copies of the supplied files:

* :download:`Datatype-property CSV <../../../resources/documentation-examples/data/datatype_test.csv>`
* :download:`Datatype-property Python script <../../../resources/documentation-examples/scripts/datatype_test.py>`

Keep the ``scripts`` and ``data`` directories beneath the same parent directory
so the script can resolve its input path.
