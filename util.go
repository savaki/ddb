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
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func makeKey(spec *tableSpec, hashKey, rangeKey interface{}) (map[string]*dynamodb.AttributeValue, error) {
	hk, err := marshal(hashKey)
	if err != nil {
		return nil, wrapf(err, ErrUnableToMarshalItem, "unable to encode hash key, %v", hashKey)
	}

	rk, err := marshal(rangeKey)
	if err != nil {
		return nil, wrapf(err, ErrUnableToMarshalItem, "unable to encode range key, %v", rangeKey)
	}

	keys := map[string]*dynamodb.AttributeValue{}
	if key := spec.HashKey; key != nil {
		keys[key.AttributeName] = hk
	}
	if key := spec.RangeKey; key != nil {
		keys[key.AttributeName] = rk
	}

	return keys, nil
}

func marshal(item interface{}) (*dynamodb.AttributeValue, error) {
	switch v := item.(type) {
	case *dynamodb.AttributeValue:
		return v, nil
	case map[string]*dynamodb.AttributeValue:
		return &dynamodb.AttributeValue{M: v}, nil
	case []*dynamodb.AttributeValue:
		return &dynamodb.AttributeValue{L: v}, nil
	default:
		return dynamodbattribute.Marshal(item)
	}
}

func marshalMap(item interface{}) (map[string]*dynamodb.AttributeValue, error) {
	switch v := item.(type) {
	case map[string]*dynamodb.AttributeValue:
		return v, nil
	default:
		return dynamodbattribute.MarshalMap(item)
	}
}
