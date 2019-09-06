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
		return errorf(ErrItemNotFound, "item not found")
	}

	if err := dynamodbattribute.UnmarshalMap(output.Item, v); err != nil {
		return err
	}

	return nil
}

func (g *Get) Scan(v interface{}) error {
	return g.ScanWithContext(defaultContext, v)
}

func (t *Table) Get(hashKey interface{}) *Get {
	return &Get{
		api:     t.ddb.api,
		spec:    t.spec,
		hashKey: hashKey,
		table:   t.consumed,
	}
}
