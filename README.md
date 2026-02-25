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

or directly from the FAIRLinked GitHub repository

```bash
git clone https://github.com/cwru-sdle/FAIRLinked.git
cd FAIRLinked
pip install .
```

---

# 👀 Public Datasets

[PMMA (poly(methyl methacrylate)) dataset](https://doi.org/10.17605/OSF.IO/9HPGW) generated using FAIRLinked. 

---

## ⏰ Quick Start

This section provides example runs of the serialization and deserialization processes. All example files can be found in the GitHub repository of `FAIRLinked` under `resources` or can be directly accessed [here](https://github.com/cwru-sdle/FAIRLinked/blob/main/resources). Command-line version of the functions below can be found [here](https://github.com/cwru-sdle/FAIRLinked/blob/main/resources/CLI_Examples.md) and in [updates](https://github.com/cwru-sdle/FAIRLinked/blob/main/UPDATES.md).

### Serializing and deserializing with RDFTableConversion

To start serializing with FAIRLinked, we first make a template using `jsonld_template_generator` from `FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper`. In your CSV, make sure to have some (possibly empty or partially filled) rows reserved for metadata about your variable. 

**Note**
Please make sure to follow the proper formatting guidelines for input CSV file. 
 * Each column name should be the "common" or alternative name for this object
 * The following three rows should be reserved for the **type**, **units**, and **study stage** in that order
 * if values for these are not available, the space should be left blank
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

**IN THIS FIRST EXAMPLE**, we will use the microindentation data of a PMMA, or Poly(methyl methacrylate), sample.

```python
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from FAIRLinked.RDFTableConversion.csv_to_jsonld_mapper import jsonld_template_generator

mds_graph = load_mds_ontology_graph()

jsonld_template_generator(csv_path="resources/worked-example-RDFTableConversion/microindentation/sa17455_00.csv", 
                           ontology_graph=mds_graph, 
                           output_path="resources/worked-example-RDFTableConversion/microindentation/output_template.json", 
                           matched_log_path="resources/worked-example-RDFTableConversion/microindentation/microindentation_matched.txt", 
                           unmatched_log_path="resources/worked-example-RDFTableConversion/microindentation/microindentation_unmatched.txt",
                           skip_prompts=False)

```

The template is designed to caputre the metadata associated with a variable, including units, study stage, row key, and variable definition. If the user do not wish to go through the prompts to put in the metadata, set `skip_prompts` to `True`.

After creating the template, run `extract_data_from_csv` using the template and CSV input to create JSON-LDs filled with data instances.

```python
from FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler import extract_data_from_csv
import json
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph


mds_graph = load_mds_ontology_graph()

with open("resources/worked-example-RDFTableConversion/microindentation/output_template.json", "r") as f:
    metadata_template = json.load(f) 

prop_col_pair_dict = {"is about": [("PolymerGrade", "Sample"), 
                                    ("Hardness (GPa)", "Sample"),
                                    ("VickersHardness", "YoungModulus"),
                                    ("Load (Newton)", "Measurement"),
                                    ("ExposureStep","Measurement"), 
                                    ("ExposureType","Measurement"),
                                    ("MeasurementNumber","Measurement")]}

extract_data_from_csv(metadata_template=metadata_template, 
                      csv_file="resources/worked-example-RDFTableConversion/microindentation/sa17455_00.csv",
                      orcid="0000-0001-2345-6789", 
                      output_folder="resources/worked-example-RDFTableConversion/microindentation/test_data_microindentation/output_microindentation",
                      row_key_cols=["Measurement", "Sample"],
                      id_cols=["Measurement", "Sample"],
                      prop_column_pair_dict=prop_col_pair_dict,
                      ontology_graph=mds_graph,
                      license="CC0-1.0")
```

The arguments `row_key_cols`, `id_cols`, `prop_column_pair_dict`, and `ontology_graph` are all optional arguments. `row_key_cols` identify columns in which concatenation of values create row keys which are used to identify the columns, while `id_cols` are columns whose value specify identifiers of unique entities which must be kept track across multiple rows. `prop_column_pair_dict` is a Python dictionary specifying the object properties or data properties (specified by the value of `rdfs:label` of that property) which will be used in the resulting RDF graph and the instances connected by those properties. Finally, `ontology_graph` is a required argument if `prop_column_pair_dict` is provided, and this is the source of the properties available to the user. The `license` argument is an optional argument. If not provided, the function defaults to the permissive public domain license (CC0-1.0). For a list of available licenses see https://spdx.org/licenses/.

To view the list of properties in MDS-Onto, run the following script:

```python
from FAIRLinked.InterfaceMDS.load_mds_ontology import load_mds_ontology_graph
from FAIRLinked.RDFTableConversion.csv_to_jsonld_template_filler import generate_prop_metadata_dict

mds_graph = load_mds_ontology_graph()

view_all_props = generate_prop_metadata_dict(mds_graph)

for key, value in view_all_props.items():
    print(f"{key}: {value}")
```


To deserialize your data, use `jsonld_directory_to_csv`, which will turn a folder of JSON-LDs (with the same data schema) back into a CSV with metadata right below the column headers.

```python
import rdflib
from rdflib import Graph
import FAIRLinked.RDFTableConversion.jsonld_batch_converter
from FAIRLinked.RDFTableConversion.jsonld_batch_converter import jsonld_directory_to_csv

jsonld_directory_to_csv(input_dir="resources/worked-example-RDFTableConversion/microindentation/test_data_microindentation/output_microindentation",
                        output_basename="sa17455_00_microindentation",
                        output_dir="resources/worked-example-RDFTableConversion/microindentation/test_data_microindentation/output_deserialize_microindentation")
```


## Serializing and deserializing using RDF Data Cube with QBWorkflow

The RDF Data Cube Workflow is better run in `bash`.

```shell
$ FAIRLinked data-cube-run
```

This will start a series of prompts for users to serialize their data using RDF Data Cube vocabulary.

```text
Welcome to FAIRLinked RDF Data Cube 🚀
Do you have an existing RDF data cube dataset? (yes/no): no
```
Answer 'yes' to deserialize your data from linked data back to tabular format. If you do not wish to deserialize, answer 'no'. After answering 'no', you will be asked whether you are currently running an experiment. To generate a data template, answer 'yes'. Otherwise, answer 'no'.

```text
Are you running an experiment now? (yes/no): yes
```

Once you've answered 'yes', you will be prompted to provide two ontology files.

```text
Do you have these ontology files (lowest-level, MDS combined)? (yes/no): yes
```

This question is asking if you have the following two turtle files: 'lowest-level' and 'MDS combined'. A 'lowest-level' ontology file is a turtle file that contains all the terms you want to use in your dataset, while 'MDS combined' is the general MDS-Onto which can be downloaded from our website https://cwrusdle.bitbucket.io/. If you answer 'yes', you will be prompted to provide file paths to these files. If you answer 'no', then a generic template will be created with unspecified variable name.

```text
Enter the path to the Lowest-level MDS ontology file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/Low-Level_Corrected.ttl
Enter the path to the Combined MDS ontology file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/MDS_Onto_Corrected.ttl

```

This will generate a template in the current working directory. For the result of this run, see `resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/data_template.xlsx`.

To serialize your data, start the workflow again.

```shell
$ FAIRLinked data-cube-run
```

Answer 'no' to the first question.

```text
Welcome to FAIRLinked RDF Data Cube 🚀
Do you have an existing RDF data cube dataset? (yes/no): no
```

When asked if you are running an experiment, answer 'no'.

```text
Are you running an experiment now? (yes/no): no
```

For most users, the answer to the question below should be 'no'. However, if you are working with a distributed database where 'hotspotting' could be a potential problem (too many queries directed towards one node), then answering 'yes' will make sure serialized files are "salted". If you answer 'yes', each row in a single sheet will be serialized, and the file names will be "salted" with two random letters in front of the other row identifiers.

```text
Do you have data for CRADLE ingestion? (yes/no): no
```

Next, you will be prompted for a namespace template (which contains a mapping of all the prefixes to the proper namespace in the serialized files), your filled-out data file, and the path to output the serialized files.

```text
Enter ORCID iD: 0000-0001-2345-6789 
Enter the path to the namespace Excel file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/namespace_template.xlsx
Enter the path to the data Excel file: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/mock_xrd_data.xlsx
Enter the path to the output folder: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/output_serialize
```

The next question asks for the mode of conversion. If 'entire', then the full table will be serialized into one RDF graph. If 'row-by-row', the each row will be serialized into its own graph. In this example, we will choose row-by-row.

```text
Do you want to convert the entire DataFrame as one dataset or row-by-row? (entire/row-by-row): row-by-row
```

You will then be prompted to select your row identifiers and the `FAIRLinked` will automatically exits.

```text
The following columns appear to be identifiers (contain 'id' in their name):
Include column 'ExperimentId' in the row-based dataset naming? (yes/no): yes
Include column 'DetectorWidth' in the row-based dataset naming? (yes/no): no
Approved ID columns for naming: ['ExperimentId']
Conversion completed under mode='row-by-row'. Outputs in: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/output_serialize/Output_0000000123456789_20260216110630
FAIRLinked exiting
```

To deserialize your data, start the workflow and answer 'yes' to the first question:

```text
Welcome to FAIRLinked RDF Data Cube 🚀
Do you have an existing RDF data cube dataset? (yes/no): yes
```

```text
Enter the path to your RDF data cube file/folder (can be .ttl/.jsonld or a directory): resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/output_serialize/Output_0000000123456789_20260216110630/jsonld
Enter the path to the output folder: resources/worked-example-QBWorkflow/test_data/Final_Corrected_without_DetectorName/output_deserialize
```
---

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
