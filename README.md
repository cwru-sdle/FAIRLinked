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

Documentations of how to use functions in FAIRLinked can be found [here](https://fairlinked.readthedocs.io/)

---

## ✍️ Authors

* **Van D. Tran**
* **Brandon Lee**
* Ritika Lamba
* Henry Dirks
* Balashanmuga Priyan Rajamohan
* Gabriel Ponon
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

## ✨ New in v0.3.2

Version 0.3.2 now includes a command line interface for accessing functions in FAIRLinked.

-----

# FAIRLinked Command-Line Interface (CLI)

This document provides instructions for using the `FAIRLinked` command-line tool. The tool is organized into three main sub-modules: `InterfaceMDS`, `RDFTableConversion`, and `QBWorkflow`.

## General Usage

The tool is invoked from the command line using the main script name, followed by a command and its specific arguments.

```bash
FAIRLinked [COMMAND] [OPTIONS]
```

To see the help message for any command, use the `-h` or `--help` flag.

```bash
FAIRLinked filter -h
FAIRLinked generate-template --help
```

-----

## InterfaceMDS Commands

These commands are used for interacting with the Materials Data Schema (MDS) ontology.

### `filter`

Get terms associated with a certain Domain, Subdomain, or Study Stage.

**Description:**
Term search using Domain, SubDomain, or Study Stage. For a complete list of Domains and SubDomains, run `FAIRLinked view-domains` and `FAIRLinked dir-make`. The current list of Study Stages includes: Synthesis, Formulation, Materials Processing, Sample, Tool, Recipe, Result, Analysis, Modelling.

**Usage:**

```bash
FAIRLinked filter -t <SEARCH_TYPES> -q <QUERY_TERM> [OPTIONS]
```

**Arguments:**

  * `-t, --search_types`: (Required) Specifies the search criteria. Choices: `"Domain"`, `"SubDomain"`, `"Study Stage"`. You can provide one or more.
  * `-q, --query_term`: (Required) Enter the domain, subdomain, or study stage.
  * `-op, --ontology_path`: Path to the ontology file. Defaults to `"default"`.
  * `-te, --ttl_extr`: Specifies whether to save search results. Choices: `"T"` or `"F"`. Defaults to `"F"`.
  * `-tp, --ttl_path`: If saving results (`-te T`), provide the full path and filename for the output.

**Example:**
Search for the term "Chem-rxn" within the "Domain" search type.

```bash
FAIRLinked filter -t "Domain" -q "Chem-rxn"
```

Search for terms in the "Sample" Study Stage and save the results to a file.

```bash
FAIRLinked filter -t "Study Stage" -q "Sample" -te "T" -tp "/path/to/save/sample_terms.ttl"
```

### `view-domains`

Display unique Domains and SubDomains from the ontology.

**Usage:**

```bash
FAIRLinked view-domains
```

### `dir-make`

View and make a directory tree of turtle files based on domains and subdomains.

**Usage:**

```bash
FAIRLinked dir-make
```

### `add-terms`

Add new terms to an existing ontology file.

**Usage:**

```bash
FAIRLinked add-terms -op <PATH_TO_ONTOLOGY>
```

**Arguments:**

  * `-op, --onto_file_path`: Path to the ontology file you want to modify.

**Example:**

```bash
FAIRLinked add-terms -op "/path/to/my_ontology.ttl"
```

### `term-search`

Search for terms by matching term labels using a fuzzy search algorithm.

**Usage:**

```bash
FAIRLinked term-search
```

-----

## RDFTableConversion Commands

These commands facilitate the conversion of tabular data (CSV) to and from RDF (JSON-LD format).

### `generate-template`

Generate a JSON-LD template based on a CSV file.

**Description:**
This command generates a template that allows users to fill in metadata about columns in their dataframe, including units, definitions, and explanatory notes. For column labels that can be matched to a term in MDS-Onto, the definition will be pre-filled.

**Usage:**

```bash
FAIRLinked generate-template -cp <CSV_PATH> -out <OUTPUT_PATH> -lp <LOG_PATH> [OPTIONS]
```

**Arguments:**

  * `-cp, --csv_path`: (Required) Path to the input CSV file.
  * `-out, --output_path`: (Required) Path to save the output JSON-LD template file.
  * `-lp, --log_path`: (Required) Path to a directory to store log files detailing which labels were matched.
  * `-op, --ontology_path`: Path to the ontology file. Use `"default"` for the official MDS-Onto.

**Example:**

```bash
FAIRLinked generate-template -cp "/data/experiments.csv" -out "/metadata/template.json" -lp "/logs/" -op "default"
```
Find an example template.jsonld in /resources

**Note**
Please make sure to follow the proper formating guidlines for input CSV file. 
 * Each column name should be the "common" or alternative name for this object
 * The following three rows should be reserved for the **type**, **units**, and **study stage** in that order
 * if values for these are not avaible, the space should be left blank
 * data for each sample can then begin on the 5th row

 Please see the following images for reference 
 ![Full Table](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/fulltable.png)

 Minimum Viable Data
![Sparse Table](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/mintable.png)

During the template generating process, the user may be prompted for data for different columns. When no units are detected, the user will be prompted for the type of unit, and then given a list of valid units to choose from. 
![Sparse Table](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/kind.png)
![Sparse Table](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/unit.png)
When no study stage is detected, the user will similarly be given a list of study stages to choose from.
![Sparse Table](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/resources/images/studystage.png)
The user will automatically be prompted for an optional notes for each column.

### `serialize-data`

Create a directory of JSON-LD files from a single CSV file and a metadata template.

**Usage:**

```bash
FAIRLinked serialize-data -mdt <TEMPLATE_PATH> -cf <CSV_PATH> -rkc <ROW_KEY_COLS> -orc <ORCID> -of <OUTPUT_FOLDER> [OPTIONS]
```

**Arguments:**

  * `-mdt, --metadata_template`: (Required) Path to the completed JSON-LD metadata template file.
  * `-cf, --csv_file`: (Required) Path to the CSV file containing the data.
  * `-rkc, --row_key_cols`: (Optional) Comma-separated list of column names that uniquely identify rows (e.g., `"col1,col2,col3"`).
  * `-ic, --id_cols`: (Optional) Choose columns used to uniquely identify a specific entity, such as a sample, a sample set, tool etc...
  * `-orc, --orcid`: (Required) ORCID identifier of the researcher (e.g., `"0000-0001-2345-6789"`).
  * `-of, --output_folder`: (Required) Directory where the generated JSON-LD files will be saved.
  * `-pc, --prop_col`: A Python dictionary literal defining relationships between columns.
  * `-op, --ontology_path`: Path to the ontology file. Required if `-pc` is provided.
  * `-base, --base_uri`: Base URI used to construct subject and object URIs.
  * ``-l, --license``: License used, find valid licenses at https://spdx.org/licenses/.

**Example:**

```bash
FAIRLinked serialize-data \
    -mdt "/metadata/template.json" \
    -cf "/data/experiments.csv" \
    -rkc "SampleID,RunNumber" \
    -orc "0000-0001-2345-6789" \
    -of "/output/jsonld_files/"
```

find example filled out jsonlds in /resources/outdir

**Example with `-pc` argument:**

```bash
FAIRLinked serialize-data \
    -mdt "/metadata/template.json" \
    -cf "/data/experiments.csv" \
    -rkc "SampleID" \
    -ic "SampleID","MeasurementID" \
    -orc "0000-0001-2345-6789" \
    -of "/output/jsonld_files/" \
    -op "default" \
    -pc '{"hasInput": [("ProcessStep", "MaterialID")], "hasOutput":[("ProcessStep", "OutputMaterialID")]}'
```

### `deserialize-data`

Deserialize a directory of JSON-LD files back into a CSV file.

**Usage:**

```bash
FAIRLinked deserialize-data -jd <JSONLD_DIRECTORY> -on <OUTPUT_NAME> -od <OUTPUT_DIR>
```

**Arguments:**

  * `-jd, --jsonld_directory`: (Required) Directory containing the JSON-LD files.
  * `-on, --output_name`: (Required) The base name for the output files (e.g., "my\_deserialized\_data").
  * `-od, --output_dir`: (Required) Path to the directory where the output CSV will be saved.

**Example:**

```bash
FAIRLinked deserialize-data \
    -jd "/output/jsonld_files/" \
    -on "reconstructed_data" \
    -od "/data/reconstructed/"
```

An example can be found in /resources/output

**Note** 
The output CSV can be used to generate a template, or can me modified and then turned into a new template

-----

## QBWorkflow Commands

Commands related to the RDF Data Cube workflow.

### `data-cube-run`

Start the RDF Data Cube Workflow.

**Description:**
The RDF Data Cube is a comprehensive FAIRification workflow designed for users familiar with the RDF Data Cube vocabulary. This workflow supports the creation of richly structured, multidimensional datasets that adhere to linked data best practices.

**Usage:**

```bash
FAIRLinked data-cube-run
```

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

### To view domains/subdomains tree in MDS-Onto

To see domains/subdomains hierarchy in MDS-Onto, use `domain_subdomain_directory()`. 

```python
import FAIRLinked.InterfaceMDS.domain_subdomain_viewer
from FAIRLinked.InterfaceMDS.domain_subdomain_viewer import domain_subdomain_directory

domain_subdomain_directory()
```

This function also allows for the user to generate an actual file directory with sub-ontologies tagged only with a domain/subdomain

```python
import FAIRLinked.InterfaceMDS.load_mds_ontology 
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
import FAIRLinked.InterfaceMDS.domain_subdomain_viewer
from FAIRLinked.InterfaceMDS.domain_subdomain_viewer import domain_subdomain_directory


mds_graph = load_mds_ontology_graph()
domain_subdomain_directory(onto_graph=mds_graph, output_dir='path/to/output/directory')
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

Additional arguments can be put in to save the search results in a turtle file.

```python
term_search_general(query_term="Chem-Rxn", search_types=["SubDomain"],ttl_extr=1, ttl_path='path/to/output/file')
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
from FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper import jsonld_template_generator

mds_graph = Graph()
mds_graph.parse("path/to/ontology/file")

jsonld_template_generator(csv_path="path/to/data/csv", 
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
                      id_cols=["sample_id", "measurement_id"],
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
                      id_cols=["column_1", "column_7"],
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
