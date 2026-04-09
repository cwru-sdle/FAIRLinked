============================================
RDFTableConversion.MDS_DF User Guide
============================================

.. contents:: Table of Contents
   :depth: 2
   :local:

--------------------------------------------
MatDatSciDf
--------------------------------------------

The ``MatDatSciDf`` class is a semantic wrapper for Pandas DataFrames. It ensures that data is structurally valid, ontologically mapped, and attributed to a verified researcher (ORCID) before transformation into Linked Data (RDF).

Core Architecture
~~~~~~~~~~~~~~~~~

An instance of ``MatDatSciDf`` manages three synchronized components:
1. **Measurement Data**: A cleaned Pandas DataFrame.
2. **Metadata Graph**: An RDFLib Graph and JSON-LD template synchronized via the ``metadata_obj``.
3. **Semantic Relations**: A mapping of inter-column links via the ``data_relations`` manager.

Initialization & Metadata Ingestion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can initialize the firewall with a standard DataFrame. If your CSV includes the optional 3-row header (Type, Unit, Study Stage), the tracker can ingest them automatically.

.. code-block:: python

    import pandas as pd
    from fairlinked import MatDatSciDf

    df = pd.read_csv("experimental_data.csv")

    # Initialize with researcher identity
    mds_df = MatDatSciDf(
        df=df,
        metadata_template={},
        orcid="0000-0001-2345-6789",
        df_name="PMMA_Indentation_Study",
        metadata_rows=True  # Isolates the first 3 rows as semantic headers
    )

    # Generate metadata via fuzzy matching or header parsing
    template, matched, unmatched = mds_df.template_generator(skip_prompts=True)
    mds_df.metadata_template = template

Validation and Relations
~~~~~~~~~~~~~~~~~~~~~~~~

Before export, use the firewall to audit alignment and define internal links.

.. code-block:: python

    # 1. Audit alignment between data and definitions
    mds_df.validate_metadata()

    # 2. Link columns (e.g., connect Hardness to a specific Sample)
    relations = {
        "is about": [("Hardness (GPa)", "Sample_ID")],
        "mds:measuredBy": [("Hardness (GPa)", "Vickers_Indenter")]
    }
    mds_df.add_relations(relations)

Serialization (Export/Import)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Bulk Export: Aggregate all rows into one master JSON-LD
    mds_df.serialize_bulk(output_path="outputs/dataset.jsonld", license="MIT")

    # Reconstruct: Restore a MatDatSciDf object from a directory of RDF files
    reconstructed = MatDatSciDf.from_rdf_dir(input_dir="records/", orcid="0000-0001-2345-6789")

.. list-table:: MatDatSciDf API Summary
   :widths: 30 70
   :header-rows: 1

   * - Method
     - Purpose
   * - ``template_generator``
     - Maps columns to ontology terms (fuzzy-match or header-parse).
   * - ``validate_metadata``
     - Audits alignment between DataFrame and JSON-LD template.
   * - ``add_relations``
     - Connects columns together via semantic predicates.
   * - ``serialize_bulk``
     - Converts the entire dataset into a master JSON-LD file.
   * - ``save_mds_df``
     - Saves "Semantic CSVs" or Parquet/Arrow files.

--------------------------------------------
Analysis Provenance (Tracker & Group)
--------------------------------------------

The Analysis Tracking system provides a transparent "paper trail" by capturing function arguments, return values, and OS-level file system events.

AnalysisTracker: Atomic Auditing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``AnalysisTracker`` monitors a specific analysis event, generating a unique UUID and identifying the agent via ORCID.

.. code-block:: python

    from fairlinked import AnalysisTracker

    tracker = AnalysisTracker(proj_name="Hardness_Fit", home_path="./results")

    @tracker.track
    def calculate_modulus(load, depth):
        return (load / depth) * 0.75 

    # The function now logs all I/O and active file handles automatically
    calculate_modulus(10.5, 0.02)

AnalysisGroup: Batch Orchestration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For parameter sweeps or iterative processing, ``AnalysisGroup`` aggregates multiple runs into a unified dataset.

.. code-block:: python

    from fairlinked import AnalysisGroup

    group = AnalysisGroup(proj_name="Temperature_Sweep", home_path="./batch_data")

    # Run multiple tracked iterations
    for t in [300, 400, 500]:
        group.run_and_track(my_simulation_func, temp=t)


**Batch Tracking with Decorators**

.. code-block:: python

    from fairlinked import AnalysisGroup

    # 1. Initialize the Group
    group = AnalysisGroup(proj_name="Temperature_Sweep", home_path="./batch_data")

    # 2. Use the @group.track decorator
    # Each call to this function will now trigger a new AnalysisTracker internally.
    @group.track
    def my_simulation_func(temp):
        """
        Performs a simulation at a specific temperature.
        Inputs and outputs are automatically audited as separate runs.
        """
        result = temp * 0.0012 
        return {"lattice_parameter": result}

    # 3. Run multiple tracked iterations
    # Each iteration receives a unique analysis_id and standalone JSON-LD graph.
    for t in [300, 400, 500]:
        my_simulation_func(temp=t)

    # 4. Aggregate Results
    # Flatten all independent runs into a single master DataFrame.
    master_df = group.create_group_arg_df()

---

Semantic Integration
~~~~~~~~~~~~~~~~~~~~

A key feature of ``AnalysisGroup`` is its ability to transition results directly back into the Semantic Firewall.

.. code-block:: python

    # 1. Flatten all run data into one master table
    master_df = group.create_group_arg_df()

    # 2. Bridge to Semantic Firewall: Automatically generates a MatDatSciDf
    mds_obj = group.create_MatDatSciDf()

    # 3. Export a master provenance graph linking all runs
    group.save_jsonld()

.. list-table:: Provenance API Summary
   :widths: 30 70
   :header-rows: 1

   * - Method
     - Purpose
   * - ``track``
     - Decorator for automatic function I/O auditing.
   * - ``run_and_track``
     - Executes code while capturing arguments and file handles.
   * - ``create_group_arg_df``
     - Concatenates batch data into a single master DataFrame.
   * - ``create_MatDatSciDf``
     - Converts batch results into a semantic-aware MDS object.
   * - ``save_jsonld``
     - Serializes the complete provenance graph.

--------------------------------------------
License and Compliance
--------------------------------------------

Use the built-in SPDX utility to find valid licenses for your data serialization.

.. code-block:: python

    MatDatSciDf.search_license("Creative Commons")








