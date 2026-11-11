.. rst-class:: visually-hidden-title

FAIRlinked
==========

.. raw:: html

   <div class="homepage-brand" aria-label="FAIRlinked logo">
     <img class="homepage-logo only-light" src="_static/fairlinked_logo.png" alt="FAIRlinked logo">
     <img class="homepage-logo only-dark" src="_static/fairlinked_logo.png" alt="FAIRlinked logo">
   </div>

   <br>

.. toctree::
   :maxdepth: 2
   :hidden:

   user-guide
   command-line
   api-reference
   about

**Version:** |release|

**Useful links:**
`Source repository <https://github.com/cwru-sdle/FAIRLinked>`_ |
`Issue tracker <https://github.com/cwru-sdle/FAIRLinked/issues>`_ |
`MDS-Onto <https://cwrusdle.bitbucket.io/>`_ |
`PyPI <https://pypi.org/project/FAIRLinked/>`_

``FAIRlinked`` transforms tabular research data into semantically rich Linked
Data aligned with MDS-Onto. It helps materials researchers describe datasets,
record relationships and analysis provenance, and serialize results as RDF or
JSON-LD.

Use FAIRlinked to:

* attach ontology-backed metadata to pandas DataFrames;
* serialize tabular records to RDF and reconstruct them later;
* search, filter, and extend MDS-Onto terms;
* track analysis provenance; and
* build RDF Data Cube datasets through an interactive workflow.

.. grid:: 2

   .. grid-item-card::
      :img-top: _static/quick-start.svg

      Quick Start
      ^^^^^^^^^^^

      Install FAIRlinked and convert a small DataFrame into JSON-LD.

      +++

      .. button-ref:: user-guide-pages/quickstart
         :expand:
         :color: dark
         :click-parent:

         Start here

   .. grid-item-card::
      :img-top: _static/guide.svg

      User Guide
      ^^^^^^^^^^

      Choose a workflow and learn the main FAIRlinked concepts.

      +++

      .. button-ref:: user-guide
         :expand:
         :color: dark
         :click-parent:

         Read the guide

   .. grid-item-card::
      :img-top: _static/terminal.svg

      Command Line
      ^^^^^^^^^^^^

      Search MDS-Onto and convert datasets without writing Python code.

      +++

      .. button-ref:: command-line
         :expand:
         :color: dark
         :click-parent:

         View CLI commands

   .. grid-item-card::
      :img-top: _static/api.svg

      API Overview
      ^^^^^^^^^^^^

      Find the primary classes, functions, and package modules.

      +++

      .. button-ref:: api-reference
         :expand:
         :color: dark
         :click-parent:

         Browse the API
