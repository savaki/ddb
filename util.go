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
