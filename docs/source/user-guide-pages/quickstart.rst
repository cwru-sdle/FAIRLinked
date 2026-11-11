*********************
FAIRLinked Quickstart
*********************

FAIRLinked is an SDLE package for converting tabular data into knowledge graphs
in JSON-LD format and back again. It is built around `MDS-Onto
<https://cwrusdle.bitbucket.io/>`_, SDLE's open-source Materials Data Science
Ontology, which supplies the terms and relationships used in the generated
graphs.

1. Prepare your dataset
=======================

At minimum, a CSV needs a header row followed by data rows. The bundled
``basic_example.csv`` contains three columns:

.. list-table:: Basic example data
   :header-rows: 1
   :widths: 33 33 34

   * - ``Sample``
     - ``SampleID``
     - ``HoldTemperature``
   * - ``alloy-a``
     - ``S-001``
     - ``450``
   * - ``alloy-b``
     - ``S-002``
     - ``500``

Column headers should match class names in MDS-Onto when possible. A column's
ontology class can also be supplied later with ``update_metadata()``.

Optional metadata rows
----------------------

FAIRLinked also accepts three metadata rows immediately after the header:

1. RDF type
2. Unit
3. Study stage

The downloadable ``fairlinked_quickstart.csv`` demonstrates this format. When
using metadata rows, pass ``metadata_rows=True`` to ``MatDatSciDf``. The exact
``basic_example.py`` script below uses the ordinary CSV without metadata rows,
so it leaves this option at its default value of ``False``.

2. Load your data
=================

Create a Python script and read the CSV file with pandas:

.. code-block:: python

   from pathlib import Path

   import pandas as pd
   from FAIRLinked import MatDatSciDf

   ROOT = Path(__file__).resolve().parents[1]
   INPUT_CSV = ROOT / "data" / "basic_example.csv"
   OUTPUT_FILE = ROOT / "output" / "basic_example.jsonld"
   OUTPUT_FOLDER = ROOT / "output" / "basic_example_rows"

   data = pd.read_csv(INPUT_CSV)

3. Create a materials data science DataFrame
=============================================

Use ``MatDatSciDf`` to create a FAIRLinked DataFrame. Define relationships
between columns and entities with ``data_relations_dict``, using property names
from MDS-Onto.

Each relationship has the form:

**Source -> MDS-Onto property -> Target**

.. code-block:: python

   mds_df = MatDatSciDf(
       df=data,
       df_name="basic_example",
       infer_relations=False,
       data_relations_dict={
           "mds:hasIdentifier": [("Sample", "SampleID")],
           "mds:hasHoldTemperature": [("Sample", "HoldTemperature")],
       },
   )

Here, ``Sample`` is connected to ``SampleID`` through
``mds:hasIdentifier`` and to ``HoldTemperature`` through
``mds:hasHoldTemperature``.

4. Export your knowledge graph
==============================

Use ``serialize_bulk()`` to combine the dataset into one JSON-LD file:

.. code-block:: python

   graph = mds_df.serialize_bulk(
       output_path=str(OUTPUT_FILE),
       row_key_cols=["SampleID"],
       license="CC0-1.0",
   )

Alternatively, use ``serialize_row()`` to create a separate JSON-LD file for
each data row:

.. code-block:: python

   row_graphs = mds_df.serialize_row(
       output_folder=str(OUTPUT_FOLDER),
       row_key_cols=["SampleID"],
       license="CC0-1.0",
   )

Set the output paths to the desired locations. In both examples,
``row_key_cols`` identifies each row and ``license`` records the data license.
The dataset is now represented as JSON-LD knowledge graphs generated with
FAIRLinked.

Download the exact examples
===========================

These are unchanged copies of the supplied example files:

* :download:`Basic example CSV <../../../resources/documentation-examples/data/basic_example.csv>`
* :download:`Basic example Python script <../../../resources/documentation-examples/scripts/basic_example.py>`
* :download:`CSV with three metadata rows <../../../resources/documentation-examples/data/fairlinked_quickstart.csv>`

Keep the ``scripts`` and ``data`` directories beneath the same parent directory
when running the script. Create an ``output`` directory there if it does not
already exist.

Continue with :doc:`datatype-properties` or learn how to
:doc:`creating-entities`.
