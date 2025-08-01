# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
import sphinx_rtd_theme
sys.path.insert(0, os.path.abspath('../../'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'FAIRLinked'
copyright = '2025, Van D. Tran, Ritika Lamba, Balashanmuga Priyan Rajamohan, Gabriel Ponon, Kai Zheng, Benjamin Pierce, Quynh D. Tran, Ozan Dernek, Erika I. Barcelos, Roger H. French'
author = 'Van D. Tran, Ritika Lamba, Balashanmuga Priyan Rajamohan, Gabriel Ponon, Kai Zheng, Benjamin Pierce, Quynh D. Tran, Ozan Dernek, Erika I. Barcelos, Roger H. French'
release = '0.3.0.8'

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
