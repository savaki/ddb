package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Update struct {
	api            dynamodbiface.DynamoDBAPI
	spec           *tableSpec
	hashKey        Value
	rangeKey       Value
	consistentRead bool
	consumed       *ConsumedCapacity
	err            error
	updates        *expression
}

func (u *Update) makeUpdateItemInput() *dynamodb.UpdateItemInput {
	var (
		key              = makeKey(u.spec, u.hashKey, u.rangeKey)
		updateExpression = u.updates.String()
	)

	return &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  u.updates.names,
		ExpressionAttributeValues: u.updates.values,
		Key:                       key,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(u.spec.TableName),
		UpdateExpression:          aws.String(updateExpression),
	}
}

func (u *Update) Range(value Value) *Update {
	u.rangeKey = value
	return u
}

func (u *Update) Set(expr string, values ...Value) *Update {
	if err := u.updates.Set(expr, values...); err != nil {
		u.err = err
	}

	return u
}

func (u *Update) RunWithContext(ctx context.Context) error {
	if u.err != nil {
		return u.err
	}

	input := u.makeUpdateItemInput()
	output, err := u.api.UpdateItemWithContext(ctx, input)
	if err != nil {
		return err
	}

	u.consumed.add(output.ConsumedCapacity)
	return nil
}

func (u *Update) Run() error {
	return u.RunWithContext(defaultContext)
}

func (t *Table) Update(hashKey Value) *Update {
	return &Update{
		api:      t.ddb.api,
		spec:     t.spec,
		hashKey:  hashKey,
		consumed: t.consumed,
		updates: &expression{
			attributes: t.spec.Attributes,
		},
	}
}
