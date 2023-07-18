package arrow_schemagen

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v12/arrow"
)

// ArrowSchemaFromMap returns a new Arrow schema from an arbitrary
// map[string]interface{}
func ArrowSchemaFromMap(m map[string]interface{}) (*arrow.Schema, error) {
	s := traverseMap(m)
	return arrow.NewSchema(s, nil), nil
}

func traverseMap(m map[string]interface{}) []arrow.Field {
	var s []arrow.Field
	for k, v := range m {
		switch t := v.(type) {
		// map = Arrow Struct
		case map[string]interface{}:
			s = append(s, arrow.Field{Name: k, Type: arrow.StructOf(traverseMap(v.(map[string]interface{}))...)})
		// slice = Arrow List
		case []interface{}:
			if len(v.([]interface{})) > 0 {
				switch f := reflect.TypeOf(v.([]interface{})[0]); f.String() {
				// slice of map
				case "map":
					s = append(s, arrow.Field{Name: k, Type: arrow.StructOf(traverseMap(v.(map[string]interface{}))...)})
				// slice of primitive type
				default:
					s = append(s, arrow.Field{Name: k, Type: arrow.ListOf(GoPrimitiveToArrowType(f.String()))})
				}
			}
		// primitive types
		case int, uint, int32, uint32, int64, uint64, float32, float64, bool, string, complex64, complex128, nil:
			s = append(s, arrow.Field{Name: k, Type: GoPrimitiveToArrowType(fmt.Sprintf("%T", t))})
		// fallback to binary
		default:
			s = append(s, arrow.Field{Name: k, Type: arrow.BinaryTypes.Binary})
		}
	}
	return s
}

// GoPrimitiveToArrowType returns the Arrow DataType equivalent to a
// Go primitive type
//
// NOTE The intended use case is to support the generation of an Arrow schema
// from arbitrary JSON unmarshaled to a map[string]interface{}.
// The same schema would then be reused for other JSON using the same schema,
// the field containing nil in the map used as a schema template could be
// populated in the subsequent JSON messages, therefore the Go nil type is
// mapped to Arrow Binary type as a catchall to avoid losing data.
func GoPrimitiveToArrowType(goType string) arrow.DataType {
	switch goType {
	case "bool":
		return arrow.FixedWidthTypes.Boolean
	case "string":
		return arrow.BinaryTypes.String
	case "int":
		return arrow.PrimitiveTypes.Int32
	case "int8":
		return arrow.PrimitiveTypes.Int8
	case "int16":
		return arrow.PrimitiveTypes.Int16
	case "int32":
		return arrow.PrimitiveTypes.Int32
	case "int64":
		return arrow.PrimitiveTypes.Int64
	case "uint":
		return arrow.PrimitiveTypes.Uint32
	case "uint8":
		return arrow.PrimitiveTypes.Uint8
	case "uint16":
		return arrow.PrimitiveTypes.Uint16
	case "uint32":
		return arrow.PrimitiveTypes.Uint32
	case "uint64":
		return arrow.PrimitiveTypes.Uint64
	case "float32":
		return arrow.PrimitiveTypes.Float32
	case "float64":
		return arrow.PrimitiveTypes.Float64
	case "nil":
		return arrow.BinaryTypes.Binary
	default:
		return arrow.BinaryTypes.Binary
	}
}
