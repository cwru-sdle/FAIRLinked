Read through this document to practice using the FAIRLinked package

**BEGIN**

A sample csv with XRD data has been provided, to generate template, the following command may be used:
```bash
FAIRLinked generate-template -cp <CSV_PATH> -out <OUTPUT_PATH> -lp <LOG_PATH> [OPTIONS]
```

To test it yourself, try with arguments:
```bash
FAIRLinked generate-template -cp "resources/worked-example-RDFTableConversion/XRD-Ti64/input" -out "resources/worked-example-RDFTableConversion/XRD-Ti64/template.jsonld" -lp "resources/worked-example-RDFTableConversion/XRD-Ti64/ex/logs/" -op "default"
```

This creates a json-ld template of your data. It stores the strucutre of your data including, type, study-stage, units, etc.

With a input csv and template, a csv file can be serialized into a directory json-ld's corresponding to each sample in your csv

This is done with the command:
```bash
FAIRLinked serialize-data -mdt <TEMPLATE_PATH> -cf <CSV_PATH> -rkc <ROW_KEY_COLS> -orc <ORCID> -of <OUTPUT_FOLDER> [OPTIONS]
```

To continue with your generated template, use arguments:
``bash
FAIRLinked serialize-data \
    -mdt "resources/worked-example-RDFTableConversion/XRD-Ti64/template.jsonld" \
    -cf "resources/worked-example-RDFTableConversion/XRD-Ti64/input/example_input.csv" \
    -rkc "Sample" \
    -orc "0000-0001-2345-6789" \   #include a valid ORCID
    -of "resources/worked-example-RDFTableConversion/XRD-Ti64/ex/out/jsonld_files"
```bash


To deserialize json-ld directory back to csv, use the command:

```bash
FAIRLinked deserialize-data -jd <JSONLD_DIRECTORY> -on <OUTPUT_NAME> -od <OUTPUT_DIR>
```

To finish the practice, use arguments:
```bash
FAIRLinked deserialize-data \
    -jd "resources/worked-example-RDFTableConversion/XRD-Ti64/ex/out/jsonld_files" \
    -on "XRD-Ti64" \
    -od "resources/worked-example-RDFTableConversion/XRD-Ti64/output"
```


You can compare your generated files with the examples in the /ex folder
