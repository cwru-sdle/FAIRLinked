************
Installation
************

FAIRLinked supports Python 3.10 and newer. Installing it in a virtual
environment keeps its dependencies isolated from other projects.

Install from PyPI
=================

.. code-block:: console

   $ python -m venv .venv
   $ source .venv/bin/activate
   (.venv) $ python -m pip install FAIRLinked

Confirm the command-line interface is available:

.. code-block:: console

   (.venv) $ FAIRLinked --help

Development installation
========================

Clone the repository and install the package in editable mode:

.. code-block:: console

   $ git clone https://github.com/cwru-sdle/FAIRLinked.git
   $ cd FAIRLinked
   $ python -m venv .venv
   $ source .venv/bin/activate
   (.venv) $ python -m pip install -e .

To build this documentation locally, install the documentation requirements as
well:

.. code-block:: console

   (.venv) $ python -m pip install -r docs/requirements.txt

Next step
=========

Continue to the :doc:`user-guide-pages/quickstart` for a minimal
DataFrame-to-JSON-LD example.



