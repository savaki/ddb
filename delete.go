// Copyright 2019 Matt Ho
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
	api      dynamodbiface.DynamoDBAPI
	spec     *tableSpec
	hashKey  interface{}
	rangeKey interface{}
	consumed *ConsumedCapacity
	err      error
	expr     *expression
}

func (d *Delete) Condition(expr string, values ...interface{}) *Delete {
	if err := d.expr.Condition(expr, values...); err != nil {
		d.err = err
	}

	return d
}

func (d *Delete) Range(rangeKey interface{}) *Delete {
	d.rangeKey = rangeKey
	return d
}

func (d *Delete) RunWithContext(ctx context.Context) error {
	key, err := makeKey(d.spec, d.hashKey, d.rangeKey)
	if err != nil {
		return err
	}

	conditionExpression := d.expr.ConditionExpression()
	input := dynamodb.DeleteItemInput{
		ConditionExpression:       conditionExpression,
		ExpressionAttributeNames:  d.expr.Names,
		ExpressionAttributeValues: d.expr.Values,
		Key:                       key,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(d.spec.TableName),
	}

	output, err := d.api.DeleteItemWithContext(ctx, &input)
	if err != nil {
		return err
	}

	d.consumed.add(output.ConsumedCapacity)

	return nil
}

func (d *Delete) Run() error {
	return d.RunWithContext(defaultContext)
}

func (t *Table) Delete(hashKey interface{}) *Delete {
	return &Delete{
		api:      t.ddb.api,
		spec:     t.spec,
		hashKey:  hashKey,
		consumed: t.consumed,
		expr: &expression{
			attributes: t.spec.Attributes,
		},
	}
}
