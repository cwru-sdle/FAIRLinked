# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
import re
import sphinx_rtd_theme
sys.path.insert(0, os.path.abspath('../../'))

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
init_path = os.path.join(repo_root, "FAIRLinked", "__init__.py")

# Extract __version__ using regex
with open(init_path, "r") as f:
    init_content = f.read()

match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', init_content)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'FAIRLinked'
copyright = '2025, SDLE Research Center'
author = 'Van D. Tran, Brandon Lee, Ritika Lamba, Henry Dirks, Balashanmuga Priyan Rajamohan, Gabriel Ponon, Quynh D. Tran, Ozan Dernek, Erika I. Barcelos, Roger H. French'
if match:
    release = match.group(1)
else:
    release = "0.0.0"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
