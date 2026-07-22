# 0.3.3.13

Fix serialize_row and from_rdf_dir interaction

# 0.3.3.12

Create a version of run_and_track that works with reticulate package in R
Allow for in memory generation of an MDS_DF object from a list of json_lds

# 0.3.3.11

Change row key underscore to dashes

# 0.3.3.10

Change jsonld file name convention. Now file name does not include random letters and timestamps
Add the rdfs:label for serialize_bulk method

# 0.3.3.9

Add an argument in serialization that allows nodes to have rdfs:label
Update functions to accommodate new domain/subdomain predicate

# 0.3.3.7

Include unit helper data

# 0.3.3.6

Improve documentation

# 0.3.3.5

Improve QUDT unit retrieval from metadata row.

# 0.3.3.4

Automatic discovery of relations in the data based on ontology
Code refactoring: serialization and template generator utilizes MatDatSciDf class
Import detection continued development
AnalysisGroup can now accept a pre-existing AnalysisTracker instance

# 0.3.3.3

Add return statement to group analysis run and track
Add import detection functions

# 0.3.3.2

Fix invalid IRI creation in Analysis Tracker
Return error if a function doesn't run with run_and_track


# 0.3.3.1

Include a decorator for the analysis group class

# 0.3.3.0

Version 0.3.3 now includes ways to directly interact and manage metadata programmatically.

Also includes new classes to keep track of the analysis process.

# 0.3.2.13

Deal with numpy data types for when user wants to use datatype properties to connect from instances to literals

# 0.3.2.12

Force numpy data types to python types

# 0.3.2.11

Update rdflib dependency
Fix license path problem
Fix xsd namespace

# 0.3.2.10

Fix node duplication
Fix inability to serialize numpy float objects

# 0.3.2.9

Update documentation for clearer instructions on how to specify `prop_column_pair_dict`.
Fix link to `resources` folder
Fix deserialization functions
In input prompts for QBWorkflow, change 'ORC_ID' to 'ORCID iD'


# 0.3.2.8

Fix missing key "mds" in bindings.dict dictionary. This change automatically assigns "mds" to a value in bindings.dict
Fix namespace in base json-ld structure. "mds" is now binded to "https://cwrusdle.bitbucket.io/mds/" instead of "https://cwrusdle.bitbucket.io/mds#"