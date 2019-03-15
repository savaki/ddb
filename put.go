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
	err      error
	expr     *expression
}

func (p *Put) PutItemInput() (*dynamodb.PutItemInput, error) {
	item, err := marshalMap(p.value)
	if err != nil {
		return nil, err
	}

	return &dynamodb.PutItemInput{
		ConditionExpression:       p.expr.ConditionExpression(),
		Item:                      item,
		ExpressionAttributeNames:  p.expr.names,
		ExpressionAttributeValues: p.expr.values,
		TableName:                 aws.String(p.spec.TableName),
	}, nil
}

func (p *Put) Condition(expr string, values ...interface{}) *Put {
	if err := p.expr.Condition(expr, values...); err != nil {
		p.err = err
	}

	return p
}

func (p *Put) RunWithContext(ctx context.Context) error {
	input, err := p.PutItemInput()
	if err != nil {
		return err
	}

	output, err := p.api.PutItemWithContext(ctx, input)
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
		expr: &expression{
			attributes: t.spec.Attributes,
		},
	}
}
