*****************
Creating Entities
*****************

FAIRLinked can create entities in the output JSON-LD so dataset columns can be
connected to concepts from MDS-Onto. Entities can be declared explicitly or
created implicitly from relationship definitions.

Explicitly define an entity
===========================

Use ``add_entity()`` to declare a non-valued entity and specify its MDS-Onto
class:

.. code-block:: python

   mds_df.add_entity(
       name="Sample",
       rdf_type="mds:Sample",
       definition="Sample associated with this photovoltaic site",
   )

A non-valued entity is not backed by a dataset column and therefore has no
literal table value of its own. It can still be used as the subject or object of
relationships.

Connect the entity to columns
=============================

For example, **Sample -> hasIdentifier -> SampleID** associates the ``Sample``
entity with values in the ``SampleID`` column:

.. code-block:: python

   sample_relations = {
       "mds:hasIdentifier": [("Sample", "SampleID")],
       "mds:hasName": [("Sample", "SampleName")],
       "mds:hasSource": [("Sample", "Source")],
       "mds:hasComment": [("Sample", "Comment")],
       "mds:hasDate": [("Sample", "DateMade")],
       "mds:hasProject": [("Sample", "Project")],
   }

   mds_df.add_relations(sample_relations)

The supplied script serializes one JSON-LD graph for every row, using
``SampleID`` as the row key.

Implicitly define an entity
===========================

An entity can also be referenced directly in a relationship without first
calling ``add_entity()``. If its name uniquely matches a class in MDS-Onto,
FAIRLinked creates the entity automatically and adds it to the graph.

To try implicit creation with the supplied files, remove the ``add_entity()``
call and replace ``"Sample"`` with ``"XRaySample"`` in the relationship
definitions. If ``XRaySample`` uniquely matches a class in the active ontology,
FAIRLinked creates it automatically.

.. caution::

   Implicit creation requires a unique ontology-class match. Use
   ``add_entity()`` when the name is ambiguous or when you want to provide a
   specific definition.

Download the exact examples
===========================

These are unchanged copies of the supplied files:

* :download:`Entity example CSV <../../../resources/documentation-examples/data/sample_id_demo.csv>`
* :download:`Entity example Python script <../../../resources/documentation-examples/scripts/entity_example.py>`

Keep the ``scripts`` and ``data`` directories beneath the same parent directory
so the script can resolve its input path.
