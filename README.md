# FAIRLinked

FAIRLinked is a powerful tool for transforming research data into FAIR-compliant RDF. It helps you align tabular or semi-structured datasets with the MDS-Onto ontology and convert them into Linked Data formats, enhancing interoperability, discoverability, and reuse.

With FAIRLinked, you can:

- Convert CSV/Excel/JSON into RDF, JSON-LD, or OWL
- Automatically download and track the latest MDS-Onto ontology files
- Add or search terms in your ontology files with ease
- Generate metadata summaries and RDF templates
- Prepare datasets for FAIR repository submission

![FAIRLinked Subpackages](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/figs/fig1-fairlinked.png)

This tool is actively developed and maintained by the **SDLE Research Center at Case Western Reserve University** and is used in multiple federally funded projects.

---

## ✨ New in v0.3

Version 0.3 brings a major expansion of FAIRLinked's capabilities with:

- ✅ **New term addition** to ontologies (`add_ontology_term.py`)
- ✅ **Search/filter terms** in existing RDF files (`search_ontology_terms.py`)
- ✅ **Data format conversions**: CSV ⇌ JSON-LD, RDF ⇌ Table
- ✅ **Metadata extractors** for RDF subject-label-value triples
- ✅ **Namespace template generators** to assist in new dataset creation
- ✅ **Auto web scraping** to fetch the latest MDS-Onto `.ttl`, `.jsonld`, `.nt`, and `.owl` files from the official Bitbucket
- ✅ **Robust CLI handlers** with built-in validations and retry logic
- ✅ **Modular file outputs** including support for `.ttl`, `.jsonld`, `.owl`, `.nt`, `.csv`, `.xlsx`, `.parquet`, `.arrow`

Documentations of how to use functions in FAIRLinked can be found [here](https://fairlinked.readthedocs.io/)

---

## ✍️ Authors

* **Van D. Tran**
* **Ritika Lamba**
* **Balashanmuga Priyan Rajamohan**
* Gabriel Ponon
* Kai Zheng
* Benjamin Pierce
* Quynh D. Tran
* Ozan Dernek
* Yinghui Wu
* Erika I. Barcelos
* Roger H. French
* Laura S. Bruckman

---

## 🏢 Affiliation

Materials Data Science for Stockpile Stewardship Center of Excellence, Cleveland, OH 44106, USA

---
## 🐍 Python Installation

You can install FAIRLinked using pip:

```bash
pip install FAIRLinked
```
---

## Interface MDS Subpackage

![InterfaceMDS](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/figs/InterfaceMDSGitHub.png)


```python
import FAIRLinked.InterfaceMDS
```
Functions in Interface MDS allow users to interact with MDS-Onto and search for terms relevant to their domains. This includes loading MDS-Onto into an RDFLib Graph, view domains and subdomains, term search, and add new ontology terms to a local copy.

### To load the latest version of MDS-Onto
```python
import FAIRLinked.InterfaceMDS.load_mds_ontology 
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph

mds_graph = load_mds_ontology_graph()
```

### To view domains/subdomains in MDS-Onto

Terms in MDS-Onto are categorized under domains and subdomains, groupings related to topic areas currently being researched at SDLE and collaborators. More information about domains and subdomains can be found [here](https://cwrusdle.bitbucket.io/).

```python
import FAIRLinked.InterfaceMDS.domain_subdomain_viewer
from FAIRLinked.InterfaceMDS.domain_subdomain_viewer import domain_subdomain_viewer

domain_subdomain_viewer()
```

### Search for terms in MDS-Onto

```python
import FAIRLinked.InterfaceMDS.rdf_subject_extractor
from FAIRLinked.InterfaceMDS.rdf_subject_extractor import extract_subject_details
from FAIRLinked.InterfaceMDS.rdf_subject_extractor import fuzzy_filter_subjects_strict
import FAIRLinked.InterfaceMDS.load_mds_ontology 
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph


mds_graph = load_mds_ontology_graph()
onto_dataframe = extract_subject_details(mds_graph)
search_results = fuzzy_filter_subjects_strict(df=onto_dataframe, keywords=["Detector"])

print(search_results)
```

### Find Domain, Subdomain, and Study Stages

```python
# %%
import FAIRLinked.InterfaceMDS.term_search_general
from FAIRLinked.InterfaceMDS.term_search_general import term_search_general

term_search_general(query_term="Chem-Rxn", search_types=["SubDomain"])
```

### Add a new term to Ontology

```python
import rdflib
import FAIRLinked.InterfaceMDS.add_ontology_term
from FAIRLinked.InterfaceMDS.add_ontology_term import add_term_to_ontology

add_term_to_ontology("path/to/mds-onto/file.ttl")
```

## RDF Table Conversion Subpackage

![FAIRLinkedCore](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/figs/fig2-fairlinked.png)


```python
import FAIRLinked.RDFTableConversion
```
Functions in this subpackage allow to generate a JSON-LD metadata template from a CSV with MDS-compliant terms, generate JSON-LDs filled with data and MDS semantic relationships, and then convert a directory of JSON-LDs back into tabular format. 

### Generate a JSON-LD template from CSV

```python
import rdflib
from rdflib import Graph
import FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper
from FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper import json_ld_template_generator

mds_graph = Graph()
mds_graph.parse("path/to/ontology/file")

json_ld_template_generator(csv_path="path/to/data/csv", 
                           ontology_graph=mds_graph, 
                           output_path="path/to/output/json-ld/template", 
                           matched_log_path="path/to/output/matched/terms", 
                           unmatched_log_path="path/to/output/unmatched/terms")

```

### Create JSON-LDs from CSVs

```python
import rdflib
from rdflib import Graph
import json
import FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper
from FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler import extract_data_from_csv

with open("path/to/metadata/template", "r") as f:
    metadata_template = json.load(f) 

extract_data_from_csv(metadata_template=metadata_template, 
                      csv_file="path/to/data/csv",
                      row_key_cols=["sample_id"],
                      orcid="0000-0000-0000-0000", 
                      output_folder="path/to/output/folder/json-lds")
```

### Create JSON-LDs with relationships between data instances

```python
import FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler
from FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler import extract_data_from_csv
import json
import FAIRLinked.InterfaceMDS.load_mds_ontology 
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph


mds_graph = load_mds_ontology_graph()

with open("path/to/metadata/template", "r") as f:
    metadata_template = json.load(f) 

prop_col_pair_dict = {"name of relationship specified by rdfs:label": [("column_1", "column_2")]}

extract_data_from_csv(metadata_template=metadata_template, 
                      csv_file="path/to/csv/data",
                      row_key_cols=["column_1", "column_3", "column_7"],
                      orcid="0000-0000-0000-0000", 
                      output_folder="path/to/output",
                      prop_column_pair_dict=prop_col_pair_dict,
                      ontology_graph=mds_graph)
```

### Turn JSON-LD directory back to CSV

```python
import rdflib
from rdflib import Graph
import FAIRLinked.RDFTableConversion.jsonld_batch_converter
from FAIRLinked.RDFTableConversion.jsonld_batch_converter import jsonld_directory_to_csv

jsonld_directory_to_csv(input_dir="path/to/json-ld/directory",
                        output_basename="Name-of-CSV",
                        output_dir="path/to/output/directory")
```



## RDF DataCube Workflow

```python
import FAIRLinked.QBWorkflow.rdf_data_cube_workflow as rdf_data_cube_workflow
from rdf_data_cube_workflow import rdf_data_cube_workflow_start

rdf_data_cube_workflow_start()

```

The RDF DataCube workflow turns tabular data into a format compliant with the [RDF Data Cube vocabulary](https://www.w3.org/TR/vocab-data-cube/). 


![FAIRLinked](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/FAIRLinkedv0.2.png)

## 💡 Acknowledgments

This work was supported by:

* U.S. Department of Energy’s Office of Energy Efficiency and Renewable Energy (EERE) under the Solar Energy Technologies Office (SETO) — Agreement Numbers **DE-EE0009353** and **DE-EE0009347**
* Department of Energy (National Nuclear Security Administration) — Award Number **DE-NA0004104** and Contract Number **B647887**
* U.S. National Science Foundation — Award Number **2133576**

---
## 🤝 Contributing

We welcome new ideas and community contributions! If you use FAIRLinked in your research, please **cite the project** or **reach out to the authors**.

Let us know if you'd like to include:
* Badges (e.g., PyPI version, License, Docs)
* ORCID links or contact emails
* Example datasets or a GIF walkthrough
