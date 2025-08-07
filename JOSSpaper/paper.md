---
title: 'FAIRLinked: Data FAIRification Tool for Materials Data Science'
tags:
- Python
- Ontology
- FAIR
date: "24 July 2025"
output:
  pdf_document: default
  html_document:
    df_print: paged
authors:
- name: Van D. Tran
  affiliation: 1, 3
  orcid: "0009-0008-4355-0543"
- name: Ritika Lamba
  affiliation: 2, 3
- name: Balashanmuga Priyan Rajamohan
  affiliation: 2, 3
  orcid: "0009-0003-5326-1706"
- name: Quynh D. Tran
  affiliation: 2, 3
  orcid: "0009-0006-4025-1834"
- name: Erika I. Barcelos
  affiliation: 1, 3
  orcid: "0000-0002-9273-8488"
- name: Yinghui Wu
  affiliation: 2, 3
  orcid: "0000-0003-3991-5155"
- name: Laura S. Bruckman
  affiliation: 1, 3
  orcid: "0000-0003-1271-1072"
- name: Roger H. French
  corresponding: yes
  orcid: "0000-0002-6162-0532"
  affiliation: 1, 2, 3
bibliography: paper.bib
affiliations:
- name: Department of Materials Science and Engineering, Case Western Reserve University,
    Cleveland, OH, 44106, USA
  index: 1
- name: Department of Computer and Data Sciences, Case Western Reserve University,
    Cleveland, OH, 44106, USA
  index: 2
- name: Materials Data Science for Stockpile Stewardship Center of Excellence, Cleveland,
    OH, 44106, USA
  index: 3
editor_options: 
  markdown: 
    wrap: sentence
---


# Summary
FAIRLinked is a package used for FAIRifying data in materials science using MDS-Onto, an ontology developed to serve the need of materials data science community. FAIR stands for findability, accessibility, interoperability. FAIRLinked contains 3 subpackages. One is the InterfaceMDS package, which is a package containing functions that allow users to interface with MDS-Onto. The second one is QBWorkflow, which is a data FAIRification workflow we offer to those familiar with the RDF Data Cube vocabulary. The last one is RDFTableConversion, which is a simpler FAIRification workflow that does not rely on RDF Data Cube, but rely on a JSON-LD template that includes standard JSON objects generated from the different columns of a table that the user can fill out. 

# Statement of Need

Diverse sources of materials data, from a variety of different types of experiments for a variety of different applications. Crystallography, Photovolatics, Advanced Manufacturing, Semiconductors (different material applications). Different types of experiments include IV, Suns-Voc, XRay Diffraction, Synchrotron Xray, Pyrometry, UV-Vis, FTIR, and many more. Assessing different material properties. 
In modern materials science research, thus, we are encountering the 3V problem of Big Data: Volume, Velocity, and Variety. Data must be made AI-ready. The FAIR paper outlines list of principles (findable, accessible, interoperable, reusable) that promote machine-actionability, reducing the need for human intervention. Data is also multimodal, include tables and images, time series,...

# Materials Data Science Ontology (MDS-Onto)

## Introduction



## Organizational Structure: Domain, SubDomain, and Study Stage



# Key Features

FAIRLinked involves 3 subpackages: InterfaceMDS, RDFTableConversion, QBWorkflow. 

## Interfacing with MDS-Onto

The InterfaceMDS subpackage is a suite of functions designed to allow users to get information from MDS-Onto. There are thousands of terms in MDS-Onto, making it difficult for users to find information just by looking through the turtle file or json-ld alone. The functions in this include a function to retrieve the latest MDS-Onto, string search for terms in MDS-Onto, filter terms based on domain, list the different domains and subdomains within MDS-Onto

## FAIRLinked Core Workflow

FAIRLinked Core Workflow is the workflow for turning csv to jsonld, and then jsonld back. This works by creating a metadata template that the user can fill out. These include information about units, provenance,... After the user fills out the template, and then use that template and their data csv as inputs to a function to generate jsonlds filled with data. Each row in a CSV will be turned into a JSONLD file, and all the JSONLDs coming from the same CSV will be saved in a single directory. We also provide a function that allows users to deserialize from a directory of json-lds to csv. 

## RDF Data Cube Workflow

RDF Data Cube Workflow depends on the RDF Data Cube vocabulary.

# Typical Usage

# Code Availability

The source code for FAIRLinked can be found [here](https://pypi.org/project/FAIRLinked/) or in our [GitHub repository](https://github.com/cwru-sdle/FAIRLinked). 


# Acknowledgement