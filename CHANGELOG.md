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