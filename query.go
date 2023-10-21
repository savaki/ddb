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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Query struct {
	api                dynamodbiface.DynamoDBAPI
	spec               *tableSpec
	consistentRead     bool
	lastEvaluatedKey   *map[string]*dynamodb.AttributeValue
	lastEvaluatedToken *string
	limit              int64
	selectAttributes   string
	scanIndexForward   bool
	startKey           map[string]*dynamodb.AttributeValue
	request            *ConsumedCapacity
	table              *ConsumedCapacity
	err                error
	expr               *expression
	indexName          string
	attributes         []string
}

func (t *Table) Query(expr string, values ...interface{}) *Query {
	query := &Query{
		api:   t.ddb.api,
		spec:  t.spec,
		table: t.consumed,
		expr:  newExpression(t.spec.Attributes...),
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

func (q *Query) EachWithContext(ctx context.Context, fn func(item Item) (bool, error)) (err error) {
	if q.err != nil {
		return q.err
	}

	startKey := q.startKey
	defer func() {
		if q.lastEvaluatedKey != nil {
			*q.lastEvaluatedKey = startKey
		}
		if q.lastEvaluatedToken != nil {
			switch {
			case len(startKey) == 0:
				*q.lastEvaluatedToken = ""

			default:
				data, e := json.Marshal(startKey)
				if e != nil {
					err = fmt.Errorf("failed to marshal startKey: %w", err)
				}
				*q.lastEvaluatedToken = base64.StdEncoding.EncodeToString(data)
			}
		}
	}()

	input, err := q.QueryInput()
	if err != nil {
		return err
	}

	for {
		input.ExclusiveStartKey = startKey

		output, err := q.api.QueryWithContext(ctx, input)
		if err != nil {
			return err
		}
		startKey = output.LastEvaluatedKey

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

		if startKey == nil {
			break
		}
		if q.limit > 0 {
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

// FindAll returns all record
func (q *Query) FindAll(v interface{}) error {
	return q.FindAllWithContext(defaultContext, v)
}

// FindAllWithContext returns all record using context provided
func (q *Query) FindAllWithContext(ctx context.Context, v interface{}) error {
	if v == nil {
		return nil
	}

	slice := reflect.TypeOf(v)
	if slice.Kind() != reflect.Ptr {
		return fmt.Errorf("want ptr as input, got %T", v)
	}

	slice = slice.Elem()
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("want ptr to slice as input, got %T", v)
	}
	records := reflect.New(slice).Elem()

	element := slice.Elem()
	isPtr := element.Kind() == reflect.Ptr
	if isPtr {
		element = element.Elem()
	}

	callback := func(item Item) (bool, error) {
		v := reflect.New(element).Interface()
		if err := item.Unmarshal(&v); err != nil {
			return false, nil
		}
		record := reflect.ValueOf(v)
		if !isPtr {
			record = record.Elem()
		}
		records.Set(reflect.Append(records, record))
		return true, nil
	}

	if err := q.EachWithContext(ctx, callback); err != nil {
		return err
	}

	reflect.ValueOf(v).Elem().Set(records)

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

// Limit returns at most N elements; 0 indicates return all elements
func (q *Query) Limit(limit int64) *Query {
	q.limit = limit
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
		ExclusiveStartKey:         q.startKey,
		ExpressionAttributeNames:  q.expr.Names,
		ExpressionAttributeValues: q.expr.Values,
		FilterExpression:          filterExpression,
		IndexName:                 indexName,
		KeyConditionExpression:    conditionExpression,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		ScanIndexForward:          aws.Bool(q.scanIndexForward),
		Select:                    aws.String(q.selectAttributes),
		TableName:                 aws.String(q.spec.TableName),
	}
	if q.limit > 0 {
		input.Limit = aws.Int64(q.limit)
	}
	return &input, nil
}

// Select attributes to return; defaults to dynamodb.SelectAllAttributes
func (q *Query) Select(s string) *Query {
	q.selectAttributes = s
	return q
}

// LastEvaluatedKey stores the last evaluated key into the provided value
func (q *Query) LastEvaluatedKey(lastEvaluatedKey *map[string]*dynamodb.AttributeValue) *Query {
	q.lastEvaluatedKey = lastEvaluatedKey
	return q
}

// LastEvaluatedToken stores the last evaluated key as a base64 encoded string
func (q *Query) LastEvaluatedToken(lastEvaluatedToken *string) *Query {
	q.lastEvaluatedToken = lastEvaluatedToken
	return q
}

// ScanIndexForward when true returns the values in reverse sort key order
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
func (q *Query) ScanIndexForward(enabled bool) *Query {
	q.scanIndexForward = enabled
	return q
}

// StartKey assigns the continuation key used for query pagination
func (q *Query) StartKey(startKey map[string]*dynamodb.AttributeValue) *Query {
	q.startKey = startKey
	return q
}

// StartToken encodes start key as a base64 encoded string
func (q *Query) StartToken(token string) *Query {
	if token == "" {
		return q.StartKey(nil)
	}

	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		q.err = fmt.Errorf("failed to base64 decode start token: %w", err)
		return q
	}

	var startKey map[string]*dynamodb.AttributeValue
	if err := json.Unmarshal(data, &startKey); err != nil {
		q.err = fmt.Errorf("failed to json decode start token:% w", err)
		return q
	}

	return q.StartKey(startKey)
}
