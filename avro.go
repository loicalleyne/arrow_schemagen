// Package arrow_schemagen generates an Apache Arrow schema from
// an Apache Arrow schema or from a map[string]interface{}.
package arrow_schemagen

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"
)

type record struct {
	name   string
	ofType interface{}
	fields []interface{}
}

// ArrowSchemaFromAvro returns a new Arrow schema from an Avro schema JSON.
// Assumes that Avro schema comes from OCF or Schema Registry and that
// actual fields are in fields[] of top-level object.
func ArrowSchemaFromAvro(avroSchema []byte) (*arrow.Schema, error) {
	var m map[string]interface{}
	var node record
	json.Unmarshal(avroSchema, &m)
	if _, f := m["fields"]; f {
		for _, field := range m["fields"].([]interface{}) {
			node.fields = append(node.fields, field.(map[string]interface{}))
		}
	}
	if len(node.fields) == 0 {
		return fmt.Errorf("invalid avro schema: no top level fields found")
	}
	fields := iterateFields(node.fields)
	return arrow.NewSchema(fields, nil), nil
}

func iterateFields(f []interface{}) []arrow.Field {
	var s []arrow.Field
	var n record
	for _, field := range f {
		n.name = field.(map[string]interface{})["name"].(string)
		n.ofType = field.(map[string]interface{})["type"]

		// field is of type "record"
		if nf, f := field.(map[string]interface{})["fields"]; f {
			switch nf.(type) {
			// primitive & complex types
			case map[string]interface{}:
				for _, v := range nf.(map[string]interface{})["fields"].([]interface{}) {
					n.fields = append(n.fields, v.(map[string]interface{}))
				}
			// type unions
			default:
				for _, v := range nf.([]interface{}) {
					n.fields = append(n.fields, v.(map[string]interface{}))
				}
			}
		}
		s = append(s, traverseNodes(n))
	}
	return s
}

func traverseNodes(node record) arrow.Field {
	switch node.ofType.(type) {
	case string:
		// primitive type
		if len(node.fields) == 0 {
			return arrow.Field{Name: node.name, Type: AvroPrimitiveToArrowType(node.ofType.(string))}
		} else {
			// avro "record" type, node has "fields" array
			if node.ofType.(string) == "record" {
				var n record
				n.name = node.name
				n.ofType = node.ofType
				if len(node.fields) > 0 {
					n.fields = append(n.fields, node.fields...)
				}
				return arrow.Field{Name: node.name, Type: arrow.StructOf(iterateFields(n.fields)...)}
			}
		}
	// complex types
	case map[string]interface{}:
		var n record
		n.name = node.name
		n.ofType = node.ofType.(map[string]interface{})["type"]

		// Avro "array" field type = Arrow List type
		if i, ok := node.ofType.(map[string]interface{})["items"]; ok {
			return arrow.Field{Name: node.name, Type: arrow.ListOf(AvroPrimitiveToArrowType(i.(string)))}
		}

		// Avro "enum" field type = Arrow dictionary type
		if i, ok := node.ofType.(map[string]interface{})["symbols"]; ok {
			symbols := make(map[string]string)
			for index, symbol := range i.([]string) {
				k := strconv.FormatInt(int64(index), 10)
				symbols[k] = symbol
			}
			var dt arrow.DictionaryType = arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: arrow.BinaryTypes.String, Ordered: false}
			sl := len(symbols)
			switch {
			case sl <= math.MaxUint8:
				dt.IndexType = arrow.PrimitiveTypes.Uint8
			case sl > math.MaxUint8 && sl <= math.MaxUint16:
				dt.IndexType = arrow.PrimitiveTypes.Uint16
			case sl > math.MaxUint16 && sl <= math.MaxUint32:
				dt.IndexType = arrow.PrimitiveTypes.Uint32
			}
			return arrow.Field{Name: node.name, Type: &dt, Nullable: true, Metadata: arrow.MetadataFrom(symbols)}
		}

		// Avro logical types
		if i, ok := node.ofType.(map[string]interface{})["logicalType"]; ok {
			switch i.(string) {
			// decimal type's underlying type either "fixed" or "bytes"
			// scale, a JSON integer representing the scale (optional). If not specified the scale is 0.
			// precision, a JSON integer representing the (maximum) precision of decimals stored in this type (required).
			// Precision must be a positive integer greater than zero. If the underlying type is a fixed, then the precision is limited by its size.
			// An array of length n can store at most floor(log10(28 × n - 1 - 1)) base-10 digits of precision.
			// Scale must be zero or a positive integer less than or equal to the precision.
			case "decimal":

			// A uuid logical type annotates an Avro string. The string has to conform with RFC-4122
			case "uuid":
				return arrow.Field{Name: node.name, Type: arrow.BinaryTypes.String}

			// The date logical type represents a date within the calendar, with no reference to a particular time zone or time of day.
			// A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch, 1 January 1970 (ISO calendar)
			case "date":
				return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Date32}

			// The time-millis logical type represents a time of day, with no reference to a particular calendar, time zone or date, with a precision of one millisecond.
			// A time-millis logical type annotates an Avro int, where the int stores the number of milliseconds after midnight, 00:00:00.000.
			case "time-millis":
				return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Time32ms}

			// The time-micros logical type represents a time of day, with no reference to a particular calendar, time zone or date, with a precision of one microsecond.
			// A time-micros logical type annotates an Avro long, where the long stores the number of microseconds after midnight, 00:00:00.000000.
			case "time-micros":
				return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Time64us}

			// The timestamp-micros logical type represents an instant on the global timeline, independent of a particular time zone or calendar,
			// with a precision of one microsecond.
			// A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds from
			// the unix epoch, 1 January 1970 00:00:00.000000 UTC.
			case "timestamp-micros":
				return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Timestamp_us}

			// The local-timestamp-millis logical type represents a timestamp in a local timezone, regardless of what specific time zone is considered local,
			// with a precision of one millisecond.
			// A local-timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds, from 1 January 1970 00:00:00.000.
			case "local-timestamp-millis":
				return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Timestamp_ms}

			// The local-timestamp-micros logical type represents a timestamp in a local timezone, regardless of what specific time zone is considered local,
			// with a precision of one microsecond.
			// A local-timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds, from 1 January 1970 00:00:00.000000.
			case "local-timestamp-micros":
				return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Timestamp_us}

			// The duration logical type represents an amount of time defined by a number of months, days and milliseconds.
			// This is not equivalent to a number of milliseconds, because, depending on the moment in time from which the duration is measured,
			// the number of days in the month and number of milliseconds in a day may differ.
			// Other standard periods such as years, quarters, hours and minutes can be expressed through these basic periods.
			// A duration logical type annotates Avro fixed type of size 12, which stores three little-endian unsigned integers
			// that represent durations at different granularities of time.
			// The first stores a number in months, the second stores a number in days, and the third stores a number in milliseconds.
			case "duration":
				return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.MonthDayNanoInterval}
			}
		}

		// Avro "fixed" field type = Arrow FixedSize Primitive BinaryType
		if i, ok := node.ofType.(map[string]interface{})["size"]; ok {
			return arrow.Field{Name: node.name, Type: &arrow.FixedSizeBinaryType{ByteWidth: i.(int)}}
		}
		// Avro "map" field type = Arrow Map type
		if i, ok := node.ofType.(map[string]interface{})["values"]; ok {
			return arrow.Field{Name: node.name, Type: arrow.MapOf(arrow.BinaryTypes.String, AvroPrimitiveToArrowType(i.(string)))}
		}
		// Avro "record" field type = Arrow Struct type
		if _, f := node.ofType.(map[string]interface{})["fields"]; f {
			for _, field := range node.ofType.(map[string]interface{})["fields"].([]interface{}) {
				n.fields = append(n.fields, field.(map[string]interface{}))
			}
		}
		s := iterateFields(n.fields)
		return arrow.Field{Name: n.name, Type: arrow.StructOf(s...)}

	// Avro union types
	case []interface{}:
		var unionTypes []string
		for _, ft := range node.ofType.([]interface{}) {
			switch ft.(type) {
			case string:
				if ft != "null" {
					unionTypes = append(unionTypes, ft.(string))
				}
			case map[string]interface{}:
				var n record
				n.name = node.name
				n.ofType = ft.(map[string]interface{})["type"]
				if _, f := ft.(map[string]interface{})["fields"]; f {
					for _, field := range ft.(map[string]interface{})["fields"].([]interface{}) {
						n.fields = append(n.fields, field.(map[string]interface{}))
					}
				}
				f := iterateFields(n.fields)
				return arrow.Field{Name: node.name, Type: arrow.StructOf(f...)}
			}
		}
		// Supported Avro union type is null + one other type
		if len(unionTypes) == 1 {
			return arrow.Field{Name: node.name, Type: AvroPrimitiveToArrowType(unionTypes[0])}
		} else {
			// BYTE_ARRAY is the catchall if union type is anything beyond null + one other type
			// TODO: Complex AVRO union to Arrow Dense || Sparse Union
			return arrow.Field{Name: node.name, Type: arrow.BinaryTypes.Binary}
		}
	}
	return arrow.Field{Name: node.name, Type: arrow.BinaryTypes.Binary}
}

// AvroPrimitiveToArrowType returns the Arrow DataType equivalent to a
// Avro primitive type.
//
// NOTE: Arrow Binary type is used as a catchall to avoid potential data loss.
func AvroPrimitiveToArrowType(avroFieldType string) arrow.DataType {
	switch avroFieldType {
	// int: 32-bit signed integer
	case "int":
		return arrow.PrimitiveTypes.Int32
	// long: 64-bit signed integer
	case "long":
		return arrow.PrimitiveTypes.Int64
	// float: single precision (32-bit) IEEE 754 floating-point number
	case "float":
		return arrow.PrimitiveTypes.Float32
	// double: double precision (64-bit) IEEE 754 floating-point number
	case "double":
		return arrow.PrimitiveTypes.Float64
	// bytes: sequence of 8-bit unsigned bytes
	case "bytes":
		return &arrow.FixedSizeBinaryType{ByteWidth: 8}
	// boolean: a binary value
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	// string: unicode character sequence
	case "string":
		return arrow.BinaryTypes.String
	// fallback to binary type for any unsupported type
	default:
		return arrow.BinaryTypes.Binary
	}
}
