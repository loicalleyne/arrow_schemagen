# arrow_schemagen
 Generate an Apache Arrow schema from an Avro schema or an arbitrary map
 Uses v12 of the Apache Arrow Go package: github.com/apache/arrow/go/v12/arrow

How to use:

Pass in an Avro schema JSON and receive a *arrow.Schema
```golang
import (
	"github.com/apache/arrow/go/v12/arrow"
	asg "github.com/loicalleyne/arrow_schemagen"
)

func main() {
	var avroSchema map[string]interface{}
	json.Unmarshal([]byte(avroSchemaJSON), &avroSchema)
	schema, err := ArrowSchemaFromAvro(avroSchema)
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
	schema, err := ArrowSchemaFromMap(sentReq)
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
    - resource: type=struct<id: int32, ids: list<item: binary, nullable>, external_id: utf8, width: int32, height: int32>
```