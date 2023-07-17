# arrow-schemagen
 Generate an Apache Arrow schema from an Avro schema or an arbitrary map
 Uses v12 of the Apache Arrow Go package: github.com/apache/arrow/go/v12/arrow
How to use:
Pass an Avro schema as a []byte to ArrowSchemaFromAvro(), returns *arrow.Schema