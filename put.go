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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// PutAPI defines the interface for Put operations
type PutAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

type Put struct {
	api                                 PutAPI
	spec                                *tableSpec
	value                               interface{}
	request                             *ConsumedCapacity
	table                               *ConsumedCapacity
	err                                 error
	expr                                *expression
	returnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}

func (p *Put) Condition(expr string, values ...interface{}) *Put {
	if err := p.expr.Condition(expr, values...); err != nil {
		p.err = err
	}

	return p
}

// ConsumedCapacity captures consumed capacity to the property provided
func (p *Put) ConsumedCapacity(capture *ConsumedCapacity) *Put {
	p.request = capture
	return p
}

func (p *Put) PutItemInput() (*dynamodb.PutItemInput, error) {
	if p.err != nil {
		return nil, p.err
	}

	item, err := marshalMap(p.value)
	if err != nil {
		return nil, err
	}

	tableName := p.spec.TableName
	input := dynamodb.PutItemInput{
		ConditionExpression:       p.expr.ConditionExpression(),
		Item:                      item,
		ExpressionAttributeNames:  p.expr.Names,
		ExpressionAttributeValues: p.expr.Values,
		TableName:                 &tableName,
	}
	if p.request != nil {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal
	}

	return &input, nil
}

func (p *Put) ReturnValuesOnConditionCheckFailure(value types.ReturnValuesOnConditionCheckFailure) *Put {
	p.returnValuesOnConditionCheckFailure = value
	return p
}

func (p *Put) RunWithContext(ctx context.Context) error {
	input, err := p.PutItemInput()
	if err != nil {
		return err
	}

	output, err := p.api.PutItem(ctx, input)
	if err != nil {
		return err
	}

	p.table.add(output.ConsumedCapacity)
	if p.request != nil {
		p.request.add(output.ConsumedCapacity)
	}

	return nil
}

func (p *Put) Run() error {
	return p.RunWithContext(defaultContext)
}

func (p *Put) Tx() (*types.TransactWriteItem, error) {
	input, err := p.PutItemInput()
	if err != nil {
		return nil, err
	}

	writeItem := types.TransactWriteItem{
		Put: &types.Put{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Item:                      input.Item,
			TableName:                 input.TableName,
		},
	}
	if v := p.returnValuesOnConditionCheckFailure; v != "" {
		writeItem.Put.ReturnValuesOnConditionCheckFailure = v
	}

	return &writeItem, nil
}

func (t *Table) Put(v interface{}) *Put {
	return &Put{
		api:   t.ddb.api,
		spec:  t.spec,
		value: v,
		table: t.consumed,
		expr:  newExpression(t.spec.Attributes...),
	}
}
