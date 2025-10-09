// Copyright 2020 Matt Ho
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddb

import (
	"strconv"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// getMetadata accepts the key and spec for a given table and returns the corresponding hashKey, rangeKey, and tableName
func getMetadata(key map[string]types.AttributeValue, spec *tableSpec) (hashKey, rangeKey types.AttributeValue, tableName string) {
	hashKey = key[spec.HashKey.AttributeName]
	if spec.RangeKey != nil {
		rangeKey = key[spec.RangeKey.AttributeName]
	}
	return hashKey, rangeKey, spec.TableName
}

func makeKey(spec *tableSpec, hashKey, rangeKey interface{}) (map[string]types.AttributeValue, error) {
	hk, err := marshal(hashKey)
	if err != nil {
		return nil, wrapf(err, ErrUnableToMarshalItem, "unable to encode hash key, %v", hashKey)
	}

	rk, err := marshal(rangeKey)
	if err != nil {
		return nil, wrapf(err, ErrUnableToMarshalItem, "unable to encode range key, %v", rangeKey)
	}

	keys := map[string]types.AttributeValue{}
	if key := spec.HashKey; key != nil {
		keys[key.AttributeName] = hk
	}
	if key := spec.RangeKey; key != nil {
		keys[key.AttributeName] = rk
	}

	return keys, nil
}

func marshal(item interface{}) (types.AttributeValue, error) {
	switch v := item.(type) {
	case types.AttributeValue:
		return v, nil
	case map[string]types.AttributeValue:
		return &types.AttributeValueMemberM{Value: v}, nil
	case []types.AttributeValue:
		return &types.AttributeValueMemberL{Value: v}, nil

	// Fast paths for common types - avoid reflection

	// String types
	case string:
		return &types.AttributeValueMemberS{Value: v}, nil

	// Boolean
	case bool:
		return &types.AttributeValueMemberBOOL{Value: v}, nil

	// Signed integers
	case int:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(v)}, nil
	case int8:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(v), 10)}, nil
	case int16:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(v), 10)}, nil
	case int32:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(v), 10)}, nil
	case int64:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}, nil

	// Unsigned integers
	case uint:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}, nil
	case uint8:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}, nil
	case uint16:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}, nil
	case uint32:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}, nil
	case uint64:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}, nil

	// Floating point
	case float32:
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(float64(v), 'f', -1, 32)}, nil
	case float64:
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'f', -1, 64)}, nil

	// Byte slice
	case []byte:
		return &types.AttributeValueMemberB{Value: v}, nil

	default:
		return attributevalue.Marshal(item)
	}
}

func marshalMap(item interface{}) (map[string]types.AttributeValue, error) {
	switch v := item.(type) {
	case map[string]types.AttributeValue:
		return v, nil
	default:
		return attributevalue.MarshalMap(item)
	}
}
