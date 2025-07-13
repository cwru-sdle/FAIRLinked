# FAIRLinked

FAIRLinked is a powerful tool for transforming research data into FAIR-compliant RDF using the RDF Data Cube Vocabulary. It helps you align tabular or semi-structured datasets with the MDS-Onto ontology and convert them into Linked Data formats, enhancing interoperability, discoverability, and reuse.

With FAIRLinked, you can:

- Convert CSV/Excel/JSON into RDF, JSON-LD, or OWL
- Automatically download and track the latest MDS-Onto ontology files
- Add or search terms in your ontology files with ease
- Generate metadata summaries and RDF templates
- Prepare datasets for FAIR repository submission

This tool is actively developed and maintained by the **SDLE Research Center at Case Western Reserve University** and is used in multiple federally funded projects.

---

## ✨ New in v0.3.0.0

Version 0.3.0.0 brings a major expansion of FAIRLinked's capabilities with:

- ✅ **New term addition** to ontologies (`add_ontology_term.py`)
- ✅ **Search/filter terms** in existing RDF files (`search_ontology_terms.py`)
- ✅ **Data format conversions**: CSV ⇌ JSON-LD, RDF ⇌ Table
- ✅ **Metadata extractors** for RDF subject-label-value triples
- ✅ **Namespace template generators** to assist in new dataset creation
- ✅ **Auto web scraping** to fetch the latest MDS-Onto `.ttl`, `.jsonld`, `.nt`, and `.owl` files from the official Bitbucket
- ✅ **Robust CLI handlers** with built-in validations and retry logic
- ✅ **Modular file outputs** including support for `.ttl`, `.jsonld`, `.owl`, `.nt`, `.csv`, `.xlsx`, `.parquet`, `.arrow`

---

## ✍️ Authors

* **Ritika Lamba**
* **Balashanmuga Priyan Rajamohan**
* Van Tran
* Kai Zheng
* Benjamin Pierce
* Yinghui Wu
* Laura Bruckman
* Erika I. Barcelos
* Roger H. French

---

## 🏢 Affiliation

Materials Data Science for Stockpile Stewardship Center of Excellence, Cleveland, OH 44106, USA

---
## 🐍 Python Installation

You can install FAIRLinked using pip:

```bash
pip install FAIRLinked
```

## Package Usage

```bash
FAIRLinked
```

![FAIRLinked](https://raw.githubusercontent.com/cwru-sdle/FAIRLinked/main/FAIRLinkedv0.2.png)

## 💡 Acknowledgments

This work was supported by:

* U.S. Department of Energy’s Office of Energy Efficiency and Renewable Energy (EERE) under the Solar Energy Technologies Office (SETO) — Agreement Numbers **DE-EE0009353** and **DE-EE0009347**
* Department of Energy (National Nuclear Security Administration) — Award Number **DE-NA0004104** and Contract Number **B647887**
* U.S. National Science Foundation — Award Number **2133576**

---
## 🤝 Contributing

We welcome pull requests and new ideas! If you use FAIRLinked in your research, please **cite the project** or **reach out to the authors**.

Let us know if you'd like to include:
* Badges (e.g., PyPI version, License, Docs)
* ORCID links or contact emails
* Example datasets or a GIF walkthrough

