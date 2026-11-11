# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import re
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo_root))
init_path = repo_root / "FAIRLinked" / "__init__.py"

# Extract __version__ using regex
init_content = init_path.read_text(encoding="utf-8")

match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', init_content)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "FAIRLinked"
copyright = "2026, CWRU SDLE-MDS3 Center of Excellence"
author = "SDLE Research Center at Case Western Reserve University"
if match:
    release = match.group(1)
else:
    release = "0.0.0"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx_design",
]

templates_path = ["_templates"]

# These legacy narrative pages are retained in the repository while the concise
# documentation structure is reviewed. Generated API pages remain enabled so
# Sphinx can build the module index with the active theme.
exclude_patterns = [
    "intro.rst",
    "user-guide-MDS_DF.rst",
    "modules.rst",
]



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["custom.css"]

on_read_the_docs = os.environ.get("READTHEDOCS") == "True"
read_the_docs_version = os.environ.get("READTHEDOCS_VERSION", "latest")
switcher_json_url = (
    "https://fairlinked.readthedocs.io/en/latest/_static/switcher.json"
    if on_read_the_docs
    else "/_static/switcher.json"
)

html_theme_options = {
    "logo": {
        "alt_text": "FAIRLinked - Home",
        "text": "FAIRLinked",
        "image_light": "_static/logo-light.svg",
        "image_dark": "_static/logo-dark.svg",
    },
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/cwru-sdle/FAIRLinked",
            "icon": "fa-brands fa-github fa-lg",
            "type": "fontawesome",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/FAIRLinked/",
            "icon": "fa-brands fa-python fa-lg",
            "type": "fontawesome",
        },
    ],
    "switcher": {
        "json_url": switcher_json_url,
        "version_match": read_the_docs_version,
    },
    # The deployed switcher manifest is checked by the browser. Disabling the
    # build-time network request keeps local and Read the Docs builds reliable.
    "check_switcher": False,
    "navbar_start": ["navbar-logo", "version-switcher"],
    "secondary_sidebar_items": ["page-toc"],
    "footer_start": ["sphinx-version"],
    "footer_center": ["copyright"],
    "footer_end": ["theme-version"],
}

html_baseurl = "https://fairlinked.readthedocs.io/en/latest/"
