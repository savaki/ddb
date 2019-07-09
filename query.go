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
	scanIndexForward bool
	consumed         *ConsumedCapacity
	err              error
	expr             *expression
	indexName        string
	attributes       []string
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
		Select:                    aws.String(dynamodb.SelectAllAttributes),
		TableName:                 aws.String(q.spec.TableName),
	}
	return &input, nil
}

func (q *Query) ConsistentRead(enabled bool) *Query {
	q.consistentRead = true
	return q
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

		q.consumed.add(output.ConsumedCapacity)

		startKey = output.LastEvaluatedKey
		if startKey == nil {
			break
		}
	}

	return nil
}

func (q *Query) Each(fn func(item Item) (bool, error)) error {
	return q.EachWithContext(defaultContext, fn)
}

// Filter allows for the query to be conditionally filtered
func (q *Query) Filter(expr string, values ...interface{}) *Query {
	if err := q.expr.Filter(expr, values...); err != nil {
		q.err = err
	}

	return q
}

// FirstWithContext binds the first value and returns
func (q *Query) FirstWithContext(ctx context.Context, v interface{}) error {
	return q.EachWithContext(ctx, func(item Item) (bool, error) {
		if err := item.Unmarshal(v); err != nil {
			return false, err
		}
		return false, nil
	})
}

// First binds the first value and returns
func (q *Query) First(v interface{}) error {
	return q.FirstWithContext(defaultContext, v)
}

// ScanIndexForward when true returns the values in reverse sort key order
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
func (q *Query) ScanIndexForward(enabled bool) *Query {
	q.scanIndexForward = enabled
	return q
}

func (t *Table) Query(expr string, values ...interface{}) *Query {
	query := &Query{
		api:      t.ddb.api,
		spec:     t.spec,
		consumed: t.consumed,
		expr: &expression{
			attributes: t.spec.Attributes,
		},
	}
	return query.KeyCondition(expr, values...)
}
