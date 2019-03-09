package ddb

import "github.com/aws/aws-sdk-go/service/dynamodb"

func makeKey(spec *tableSpec, hashKey, rangeKey Value) map[string]*dynamodb.AttributeValue {
	keys := map[string]*dynamodb.AttributeValue{}
	if key := spec.HashKey; key != nil {
		keys[key.AttributeName] = hashKey.item
	}
	if key := spec.RangeKey; key != nil {
		keys[key.AttributeName] = rangeKey.item
	}

	return keys
}
