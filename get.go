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
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Get struct {
	api            dynamodbiface.DynamoDBAPI
	spec           *tableSpec
	hashKey        interface{}
	rangeKey       interface{}
	consistentRead bool
	table          *ConsumedCapacity
	request        *ConsumedCapacity
}

type getTx struct {
	get   *Get
	value interface{}
}

func (g getTx) Decode(v *dynamodb.ItemResponse) error {
	if len(v.Item) == 0 {
		if tx, err := g.Tx(); err == nil {
			hashKey, rangeKey, tableName := getMetadata(tx.Get.Key, g.get.spec)
			return notFoundError(hashKey, rangeKey, tableName)
		}
		return errorf(ErrItemNotFound, "item not found")
	}
	return dynamodbattribute.UnmarshalMap(v.Item, g.value)
}

func (g getTx) Tx() (*dynamodb.TransactGetItem, error) {
	key, err := makeKey(g.get.spec, g.get.hashKey, g.get.rangeKey)
	if err != nil {
		return nil, err
	}

	return &dynamodb.TransactGetItem{
		Get: &dynamodb.Get{
			Key:       key,
			TableName: aws.String(g.get.spec.TableName),
		},
	}, nil
}

func (g *Get) ConsistentRead(enabled bool) *Get {
	g.consistentRead = true
	return g
}

// ConsumedCapacity captures consumed capacity to the property provided
func (g *Get) ConsumedCapacity(capture *ConsumedCapacity) *Get {
	g.request = capture
	return g
}

func (g *Get) GetItemInput() (*dynamodb.GetItemInput, error) {
	key, err := makeKey(g.spec, g.hashKey, g.rangeKey)
	if err != nil {
		return nil, err
	}

	return &dynamodb.GetItemInput{
		ConsistentRead:         aws.Bool(g.consistentRead),
		Key:                    key,
		TableName:              aws.String(g.spec.TableName),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}, nil
}

func (g *Get) Range(value interface{}) *Get {
	g.rangeKey = value
	return g
}

func (g *Get) ScanWithContext(ctx context.Context, v interface{}) error {
	input, err := g.GetItemInput()
	if err != nil {
		return err
	}

	output, err := g.api.GetItemWithContext(ctx, input)
	if err != nil {
		return err
	}

	g.table.add(output.ConsumedCapacity)
	if g.request != nil {
		g.request.add(output.ConsumedCapacity)
	}

	if len(output.Item) == 0 {
		hashKey, rangeKey, tableName := getMetadata(input.Key, g.spec)
		return notFoundError(hashKey, rangeKey, tableName)
	}

	if err := dynamodbattribute.UnmarshalMap(output.Item, v); err != nil {
		return err
	}

	return nil
}

func (g *Get) Scan(v interface{}) error {
	return g.ScanWithContext(defaultContext, v)
}

func (g *Get) ScanTx(v interface{}) GetTx {
	return getTx{
		get:   g,
		value: v,
	}
}

func (t *Table) Get(hashKey interface{}) *Get {
	return &Get{
		api:     t.ddb.api,
		spec:    t.spec,
		hashKey: hashKey,
		table:   t.consumed,
	}
}
