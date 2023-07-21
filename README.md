# arrow_schemagen
 Generate an Apache Arrow schema from an Avro schema or an arbitrary map.
 Use with Apache Arrow Go package v13

How to use:

Pass in an Avro schema JSON and receive a *arrow.Schema
```golang
import (
	"github.com/apache/arrow/go/v13/arrow"
	asg "github.com/loicalleyne/arrow_schemagen"
)

func main() {
	var avroSchema map[string]interface{}
	json.Unmarshal([]byte(avroSchemaJSON), &avroSchema)
	//
	// ArrowSchemaFromAvro returns a new Arrow schema from an Avro schema JSON.
	// If the top level is of record type, set includeTopLevel to either make
	// its fields top level fields in the resulting schema or nested in a single field.
	//
	schema, err := asg.ArrowSchemaFromAvro(avroSchema, false)
	if err != nil {
		// deal with error
	}
	fmt.Printf("%v\n", schema.String())
}
```

Pass in a map[string]interface{} and receive a *arrow.Schema
```golang
import (
	"github.com/apache/arrow/go/v12/arrow"
	asg "github.com/loicalleyne/arrow_schemagen"
)

func main() {
	var sentReq = map[string]interface{}{
		"request": map[string]interface{}{
			"datetime":    "2021-07-27 02:59:59",
			"ip":          "34.67.160.53",
			"host":        "domain.com",
			"uri":         "/api/v1/get_stuff/xml",
			"request_uri": "/api/v1/get_stuff/xml",
			"referer":     "",
			"useragent":   "",
		},
		"resource": map[string]interface{}{
			"id": 86233,
			"ids": []interface{}{
				132, 453535, 13412341,
			},
			"external_id": "string:215426709",
			"width":       1080,
			"height":      1920,
		},
	}
	schema, err := asg.ArrowSchemaFromMap(sentReq)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n\n", schema.String())
}
```
```sh
schema:
  fields: 2
    - request: type=struct<request_uri: utf8, referer: utf8, useragent: utf8, datetime: utf8, ip: utf8, host: utf8, uri: utf8>
    - resource: type=struct<ids: list<item: int64, nullable>, external_id: utf8, width: int64, height: int64, id: int64>
```