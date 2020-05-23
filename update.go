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
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Update encapsulates the UpdateItem action
type Update struct {
	api                                 dynamodbiface.DynamoDBAPI
	spec                                *tableSpec
	hashKey                             interface{}
	rangeKey                            interface{}
	consistentRead                      bool
	request                             *ConsumedCapacity
	table                               *ConsumedCapacity
	err                                 error
	expr                                *expression
	newValues                           interface{}
	oldValues                           interface{}
	returnValuesOnConditionCheckFailure string
}

func (u *Update) returnValues() (string, error) {
	if u.newValues == nil && u.oldValues == nil {
		return dynamodb.ReturnValueNone, nil
	} else if u.newValues != nil && u.oldValues != nil {
		return "", fmt.Errorf("either NewValues or OldValues may be specified, but not both")
	} else if u.newValues != nil {
		return dynamodb.ReturnValueAllNew, nil
	} else {
		return dynamodb.ReturnValueAllOld, nil
	}
}

// Add updates a number or a set
func (u *Update) Add(expr string, values ...interface{}) *Update {
	if err := u.expr.Add(expr, values...); err != nil {
		u.err = err
	}

	return u
}

// Condition applies a condition to the update.  When called multiple
// times, the conditions will be and-ed with each other.
func (u *Update) Condition(expr string, values ...interface{}) *Update {
	if err := u.expr.Condition(expr, values...); err != nil {
		u.err = err
	}

	return u
}

func (u *Update) ConsumedCapacity(capture *ConsumedCapacity) *Update {
	u.request = capture
	return u
}

// Delete deletes elements from a set
func (u *Update) Delete(expr string, values ...interface{}) *Update {
	if err := u.expr.Delete(expr, values...); err != nil {
		u.err = err
	}

	return u
}

// Tx returns *dynamodb.TransactWriteItem suitable for use in a transaction
func (u *Update) Tx() (*dynamodb.TransactWriteItem, error) {
	input, err := u.UpdateItemInput()
	if err != nil {
		return nil, err
	}

	writeItem := dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Key:                       input.Key,
			TableName:                 input.TableName,
			UpdateExpression:          input.UpdateExpression,
		},
	}
	if v := u.returnValuesOnConditionCheckFailure; v != "" {
		writeItem.Update.ReturnValuesOnConditionCheckFailure = aws.String(v)
	}

	return &writeItem, nil
}

func (u *Update) NewValues(v interface{}) *Update {
	u.newValues = v

	return u
}

// OldValues captures the old values into the provided value
func (u *Update) OldValues(v interface{}) *Update {
	u.oldValues = v

	return u
}

// Range specifies the optional range key for the update
func (u *Update) Range(rangeKey interface{}) *Update {
	u.rangeKey = rangeKey
	return u
}

// Remove an attribute from an Item
func (u *Update) Remove(expr string, values ...interface{}) *Update {
	if err := u.expr.Remove(expr, values...); err != nil {
		u.err = err
	}

	return u
}

func (u *Update) ReturnValuesOnConditionCheckFailure(value string) *Update {
	u.returnValuesOnConditionCheckFailure = value
	return u
}

// RunWithContext invokes the update command using the provided context
func (u *Update) RunWithContext(ctx context.Context) error {
	if u.err != nil {
		return u.err
	}

	input, err := u.UpdateItemInput()
	if err != nil {
		return err
	}

	output, err := u.api.UpdateItemWithContext(ctx, input)
	if err != nil {
		return err
	}

	if m := output.Attributes; m != nil {
		if u.oldValues != nil {
			if err := dynamodbattribute.UnmarshalMap(m, u.oldValues); err != nil {
				return fmt.Errorf("update unable to unmarshal old values: %v", err)
			}
		} else if u.newValues != nil {
			if err := dynamodbattribute.UnmarshalMap(m, u.newValues); err != nil {
				return fmt.Errorf("update unable to unmarshal new values: %v", err)
			}
		}
	}

	u.table.add(output.ConsumedCapacity)
	if u.request != nil {
		u.request.add(output.ConsumedCapacity)
	}

	return nil
}

func (u *Update) Run() error {
	return u.RunWithContext(defaultContext)
}

func (u *Update) Set(expr string, values ...interface{}) *Update {
	if err := u.expr.Set(expr, values...); err != nil {
		u.err = err
	}

	return u
}

func (u *Update) UpdateItemInput() (*dynamodb.UpdateItemInput, error) {
	key, err := makeKey(u.spec, u.hashKey, u.rangeKey)
	if err != nil {
		return nil, err
	}

	returnValues, err := u.returnValues()
	if err != nil {
		return nil, err
	}

	var (
		conditionExpression = u.expr.ConditionExpression()
		updateExpression    = u.expr.UpdateExpression()
	)

	return &dynamodb.UpdateItemInput{
		ConditionExpression:       conditionExpression,
		ExpressionAttributeNames:  u.expr.Names,
		ExpressionAttributeValues: u.expr.Values,
		Key:                       key,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		ReturnValues:              aws.String(returnValues),
		TableName:                 aws.String(u.spec.TableName),
		UpdateExpression:          updateExpression,
	}, nil
}

func (t *Table) Update(hashKey interface{}) *Update {
	return &Update{
		api:     t.ddb.api,
		spec:    t.spec,
		hashKey: hashKey,
		table:   t.consumed,
		expr: &expression{
			attributes: t.spec.Attributes,
		},
	}
}
