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

type Query struct {
	api              dynamodbiface.DynamoDBAPI
	spec             *tableSpec
	consistentRead   bool
	selectAttributes string
	scanIndexForward bool
	request          *ConsumedCapacity
	table            *ConsumedCapacity
	err              error
	expr             *expression
	indexName        string
	attributes       []string
}

func (t *Table) Query(expr string, values ...interface{}) *Query {
	query := &Query{
		api:   t.ddb.api,
		spec:  t.spec,
		table: t.consumed,
		expr: &expression{
			attributes: t.spec.Attributes,
		},
	}
	return query.KeyCondition(expr, values...)
}

// ConsumedCapacity captures consumed capacity to the property provided
func (q *Query) ConsumedCapacity(capture *ConsumedCapacity) *Query {
	q.request = capture
	return q
}

func (q *Query) ConsistentRead(enabled bool) *Query {
	q.consistentRead = true
	return q
}

func (q *Query) Each(fn func(item Item) (bool, error)) error {
	return q.EachWithContext(defaultContext, fn)
}

func (q *Query) EachWithContext(ctx context.Context, fn func(item Item) (bool, error)) error {
	if q.err != nil {
		return q.err
	}

	input, err := q.QueryInput()
	if err != nil {
		return err
	}

	var startKey map[string]*dynamodb.AttributeValue
	for {
		input.ExclusiveStartKey = startKey

		output, err := q.api.QueryWithContext(ctx, input)
		if err != nil {
			return err
		}

		item := baseItem{}
		for _, rawItem := range output.Items {
			item.raw = rawItem
			ok, err := fn(item)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}

		q.table.add(output.ConsumedCapacity)
		if q.request != nil {
			q.request.add(output.ConsumedCapacity)
		}

		startKey = output.LastEvaluatedKey
		if startKey == nil {
			break
		}
	}

	return nil
}

// Filter allows for the query to be conditionally filtered
func (q *Query) Filter(expr string, values ...interface{}) *Query {
	if err := q.expr.Filter(expr, values...); err != nil {
		q.err = err
	}

	return q
}

// First binds the first value and returns
func (q *Query) First(v interface{}) error {
	return q.FirstWithContext(defaultContext, v)
}

// FirstWithContext binds the first value and returns
func (q *Query) FirstWithContext(ctx context.Context, v interface{}) error {
	var found bool
	callback := func(item Item) (bool, error) {
		if err := item.Unmarshal(v); err != nil {
			return false, err
		}
		found = true
		return false, nil
	}
	if err := q.EachWithContext(ctx, callback); err != nil {
		return err
	}
	if !found {
		return errorf(ErrItemNotFound, "item not found")
	}
	return nil
}

func (q *Query) IndexName(indexName string) *Query {
	q.indexName = indexName
	return q
}

func (q *Query) KeyCondition(expr string, values ...interface{}) *Query {
	if err := q.expr.Condition(expr, values...); err != nil {
		q.err = err
	}

	return q
}

// QueryInput returns the raw dynamodb QueryInput that will be submitted
func (q *Query) QueryInput() (*dynamodb.QueryInput, error) {
	if q.err != nil {
		return nil, q.err
	}

	var indexName *string
	if q.indexName != "" {
		indexName = aws.String(q.indexName)
	}

	if q.selectAttributes == "" {
		q.selectAttributes = dynamodb.SelectAllAttributes
	}

	conditionExpression := q.expr.ConditionExpression()
	filterExpression := q.expr.FilterExpression()
	input := dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(q.consistentRead),
		KeyConditionExpression:    conditionExpression,
		ExpressionAttributeNames:  q.expr.Names,
		ExpressionAttributeValues: q.expr.Values,
		FilterExpression:          filterExpression,
		IndexName:                 indexName,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		ScanIndexForward:          aws.Bool(q.scanIndexForward),
		Select:                    aws.String(q.selectAttributes),
		TableName:                 aws.String(q.spec.TableName),
	}
	return &input, nil
}

// Select attributes to return; defaults to dynamodb.SelectAllAttributes
func (q *Query) Select(s string) *Query {
	q.selectAttributes = s
	return q
}

// ScanIndexForward when true returns the values in reverse sort key order
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
func (q *Query) ScanIndexForward(enabled bool) *Query {
	q.scanIndexForward = enabled
	return q
}
