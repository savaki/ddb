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
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Delete struct {
	api                                 dynamodbiface.DynamoDBAPI
	spec                                *tableSpec
	hashKey                             interface{}
	rangeKey                            interface{}
	table                               *ConsumedCapacity
	request                             *ConsumedCapacity
	err                                 error
	expr                                *expression
	returnValuesOnConditionCheckFailure string
}

func (d *Delete) Condition(expr string, values ...interface{}) *Delete {
	if err := d.expr.Condition(expr, values...); err != nil {
		d.err = err
	}

	return d
}

// ConsumedCapacity captures consumed capacity to the property provided
func (d *Delete) ConsumedCapacity(capture *ConsumedCapacity) *Delete {
	d.request = capture
	return d
}

func (d *Delete) DeleteItemInput() (*dynamodb.DeleteItemInput, error) {
	key, err := makeKey(d.spec, d.hashKey, d.rangeKey)
	if err != nil {
		return nil, err
	}

	conditionExpression := d.expr.ConditionExpression()
	return &dynamodb.DeleteItemInput{
		ConditionExpression:       conditionExpression,
		ExpressionAttributeNames:  d.expr.Names,
		ExpressionAttributeValues: d.expr.Values,
		Key:                       key,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(d.spec.TableName),
	}, nil
}

// Use ReturnValuesOnConditionCheckFailure to get the item attributes if the
// Delete condition fails. For ReturnValuesOnConditionCheckFailure, the valid
// values are: NONE and ALL_OLD.
//
// Only used by Tx()
func (d *Delete) ReturnValuesOnConditionCheckFailure(value string) *Delete {
	d.returnValuesOnConditionCheckFailure = value
	return d
}

func (d *Delete) Range(rangeKey interface{}) *Delete {
	d.rangeKey = rangeKey
	return d
}

func (d *Delete) RunWithContext(ctx context.Context) error {
	input, err := d.DeleteItemInput()
	if err != nil {
		return err
	}

	output, err := d.api.DeleteItemWithContext(ctx, input)
	if err != nil {
		return err
	}

	d.table.add(output.ConsumedCapacity)
	if d.request != nil {
		d.request.add(output.ConsumedCapacity)
	}

	return nil
}

func (d *Delete) Run() error {
	return d.RunWithContext(defaultContext)
}

func (d *Delete) Tx() (*dynamodb.TransactWriteItem, error) {
	input, err := d.DeleteItemInput()
	if err != nil {
		return nil, err
	}

	writeItem := dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Key:                       input.Key,
			TableName:                 input.TableName,
		},
	}
	if v := d.returnValuesOnConditionCheckFailure; v != "" {
		writeItem.Delete.ReturnValuesOnConditionCheckFailure = aws.String(v)
	}

	return &writeItem, nil
}

func (t *Table) Delete(hashKey interface{}) *Delete {
	return &Delete{
		api:     t.ddb.api,
		spec:    t.spec,
		hashKey: hashKey,
		table:   t.consumed,
		expr: &expression{
			attributes: t.spec.Attributes,
		},
	}
}
