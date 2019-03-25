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
	hashKey          interface{}
	rangeKey         interface{}
	consistentRead   bool
	scanIndexForward bool
	consumed         *ConsumedCapacity
	err              error
	expr             *expression
	indexName        string
	attributes       []string
}

func (q *Query) QueryInput() *dynamodb.QueryInput {
	var indexName *string
	if q.indexName != "" {
		indexName = aws.String(q.indexName)
	}

	conditionExpression := q.expr.ConditionExpression()
	input := dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(q.consistentRead),
		KeyConditionExpression:    conditionExpression,
		ExpressionAttributeNames:  q.expr.Names,
		ExpressionAttributeValues: q.expr.Values,
		IndexName:                 indexName,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		ScanIndexForward:          aws.Bool(q.scanIndexForward),
		Select:                    aws.String(dynamodb.SelectAllAttributes),
		TableName:                 aws.String(q.spec.TableName),
	}
	return &input
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

	var startKey map[string]*dynamodb.AttributeValue
	var input = q.QueryInput()
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

func (q *Query) ScanIndexForward(enabled bool) *Query {
	q.scanIndexForward = enabled
	return q
}

func (t *Table) Query(hashKey interface{}) *Query {
	return &Query{
		api:      t.ddb.api,
		spec:     t.spec,
		hashKey:  hashKey,
		consumed: t.consumed,
		expr: &expression{
			attributes: t.spec.Attributes,
		},
	}
}
