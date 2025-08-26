package schema

import "github.com/hamba/avro/v2"

func AvroEncodeFn(s avro.Schema) func(v any) ([]byte, error) {
	return func(v any) ([]byte, error) {
		return avro.Marshal(s, v)
	}
}

func AvroDecodeFn(s avro.Schema) func([]byte, any) error {
	return func(data []byte, v any) error {
		return avro.Unmarshal(s, data, v)
	}
}
