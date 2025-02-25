import re

NAMESPACE_MAP = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "void": "http://rdfs.org/ns/void#",
    "dct": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "org": "http://www.w3.org/ns/org#",
    "admingeo": "http://data.ordnancesurvey.co.uk/ontology/admingeo/",
    "interval": "http://reference.data.gov.uk/def/intervals/",
    "qb": "http://purl.org/linked-data/cube#",
    "qudt": "http://qudt.org/2.1/vocab/unit",
    "mds": "https://cwrusdle.bitbucket.io/mds#",
    "sdmx-concept": "http://purl.org/linked-data/sdmx/2009/concept#",
    "sdmx-dimension": "http://purl.org/linked-data/sdmx/2009/dimension#",
    "sdmx-attribute": "http://purl.org/linked-data/sdmx/2009/attribute#",
    "sdmx-measure": "http://purl.org/linked-data/sdmx/2009/measure#",
    "sdmx-metadata": "http://purl.org/linked-data/sdmx/2009/metadata#",
    "sdmx-code": "http://purl.org/linked-data/sdmx/2009/code#",
    "sdmx-subject": "http://purl.org/linked-data/sdmx/2009/subject#",
    "ex-geo": "http://example.org/geo#",
    "eg": "http://example.org/ns#"
}

# === Instruction Texts and Messages ===
ALT_LABEL_INSTR = "Alternative Label - write mappings to the columns that you use in your dataset"
UNIT_INSTR = "Indicate what unit you are using to qualify your measure. Refer to https://www.qudt.org/doc/DOC_VOCAB-UNITS.html"
IS_MEASURE_INSTR = "If Measure indicate YES, else (dimension) indicate NO"
EXISTING_URI_INSTR = "If you already have a URI for this particular column, indicate it like so mds:InstrumentId. Ensure mds is defined in the other sheet"
EXPERIMENT_ID_INSTR = "If you don't mention an ExperimentId, IDs will be generated for each experiment"
NO_TERMS_MSG = "No matching terms found in the ontology. A default template has been generated for your use case."
TEMPLATE_GENERATED_MSG = "Excel template has been successfully generated at '{}'."

# === Column Headers ===
HEADER_NAMESPACE = "Namespace you are using"
HEADER_BASE_URI = "Base URI"

def validate_orcid_format(orcid):
    # Basic ORCID format validation (e.g., 0000-0000-0000-0000)
    pattern = r"^\d{4}-\d{4}-\d{4}-\d{4}$"
    return re.match(pattern, orcid) is not None

# Light color palette for category mapping
LIGHT_COLORS = {
    "red": "FFE6E6",      # Very Light Red
    "green": "E6FFE6",    # Very Light Green
    "blue": "E6E6FF",     # Very Light Blue
    "yellow": "FFFCE6",   # Very Light Yellow
    "orange": "FFF5E6",   # Very Light Orange
    "purple": "FFE6FF",   # Very Light Purple
    "cyan": "E6FFFF",     # Very Light Cyan
    "pink": "FFE6F0",     # Very Light Pink
    "mint": "E6FFF0",     # Very Light Mint
    "lavender": "F0E6FF"  # Very Light Lavender
}

# These will be populated by mds_ontology_analyzer
CATEGORY_COLORS = {}
ONTO_CORE_CATEGORIES = set()

