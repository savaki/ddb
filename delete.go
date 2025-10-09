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

// Delete represents a DynamoDB DeleteItem operation with support for conditions
// and consumed capacity tracking
type Delete struct {
	api                                 DynamoDBAPI
	spec                                *tableSpec
	hashKey                             interface{}
	rangeKey                            interface{}
	table                               *ConsumedCapacity
	request                             *ConsumedCapacity
	err                                 error
	expr                                *expression
	returnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}

// Condition adds a condition expression to the delete operation.
// The expression uses placeholders (#field for attribute names, ? for values).
// Multiple conditions are combined with AND.
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

// DeleteItemInput constructs and returns the underlying DynamoDB DeleteItemInput
// that will be used for the delete operation
func (d *Delete) DeleteItemInput() (*dynamodb.DeleteItemInput, error) {
	if d.err != nil {
		return nil, d.err
	}

	key, err := makeKey(d.spec, d.hashKey, d.rangeKey)
	if err != nil {
		return nil, err
	}

	tableName := d.spec.TableName
	conditionExpression := d.expr.ConditionExpression()
	return &dynamodb.DeleteItemInput{
		ConditionExpression:       conditionExpression,
		ExpressionAttributeNames:  d.expr.Names,
		ExpressionAttributeValues: d.expr.Values,
		Key:                       key,
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityTotal,
		TableName:                 &tableName,
	}, nil
}

// ReturnValuesOnConditionCheckFailure specifies what values to return when the delete condition fails.
// Valid values are types.ReturnValuesOnConditionCheckFailureNone and types.ReturnValuesOnConditionCheckFailureAllOld.
// This setting is only used when the delete is part of a transaction via Tx().
func (d *Delete) ReturnValuesOnConditionCheckFailure(value types.ReturnValuesOnConditionCheckFailure) *Delete {
	d.returnValuesOnConditionCheckFailure = value
	return d
}

// Range specifies the range key value for the item to delete.
// This is required for tables that have a composite primary key.
func (d *Delete) Range(rangeKey interface{}) *Delete {
	d.rangeKey = rangeKey
	return d
}

// RunWithContext executes the delete operation using the provided context.
// Returns an error if the delete fails or if a condition is not met.
func (d *Delete) RunWithContext(ctx context.Context) error {
	input, err := d.DeleteItemInput()
	if err != nil {
		return err
	}

	output, err := d.api.DeleteItem(ctx, input)
	if err != nil {
		return err
	}

	d.table.add(output.ConsumedCapacity)
	if d.request != nil {
		d.request.add(output.ConsumedCapacity)
	}

	return nil
}

// Run executes the delete operation using the default context.
// Returns an error if the delete fails or if a condition is not met.
func (d *Delete) Run() error {
	return d.RunWithContext(defaultContext)
}

// Tx converts the delete operation into a TransactWriteItem for use in transactions.
// Returns an error if the delete configuration is invalid.
func (d *Delete) Tx() (*types.TransactWriteItem, error) {
	input, err := d.DeleteItemInput()
	if err != nil {
		return nil, err
	}

	writeItem := types.TransactWriteItem{
		Delete: &types.Delete{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Key:                       input.Key,
			TableName:                 input.TableName,
		},
	}
	if v := d.returnValuesOnConditionCheckFailure; v != "" {
		writeItem.Delete.ReturnValuesOnConditionCheckFailure = v
	}

	return &writeItem, nil
}

// Delete creates a new Delete operation for the item with the specified hash key.
// Use Range() to specify the range key if the table has a composite primary key.
func (t *Table) Delete(hashKey interface{}) *Delete {
	return &Delete{
		api:     t.ddb.api,
		spec:    t.spec,
		hashKey: hashKey,
		table:   t.consumed,
		expr:    newExpression(t.spec.Attributes...),
	}
}
