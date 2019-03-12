package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Put struct {
	api      dynamodbiface.DynamoDBAPI
	spec     *tableSpec
	value    interface{}
	consumed *ConsumedCapacity
}

func (p *Put) RunWithContext(ctx context.Context) error {
	item, err := marshalMap(p.value)
	if err != nil {
		return err
	}

	input := dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(p.spec.TableName),
	}
	output, err := p.api.PutItemWithContext(ctx, &input)
	if err != nil {
		return err
	}

	p.consumed.add(output.ConsumedCapacity)

	return nil
}

func (p *Put) Run() error {
	return p.RunWithContext(defaultContext)
}

func (t *Table) Put(v interface{}) *Put {
	return &Put{
		api:      t.ddb.api,
		spec:     t.spec,
		value:    v,
		consumed: t.consumed,
	}
}
