package ddb

import (
	"flag"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var runIntegrationTests bool

func init() {
	flag.BoolVar(&runIntegrationTests, "integration", false, "run integration tests")
}

type Mock struct {
	dynamodbiface.DynamoDBAPI
	err      error
	getItem  interface{}
	getInput *dynamodb.GetItemInput
	putInput *dynamodb.PutItemInput
}

func (m *Mock) CreateTableWithContext(aws.Context, *dynamodb.CreateTableInput, ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, m.err
}

func (m *Mock) DeleteTableWithContext(aws.Context, *dynamodb.DeleteTableInput, ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return &dynamodb.DeleteTableOutput{}, m.err
}

func (m *Mock) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	m.getInput = input

	item, err := dynamodbattribute.MarshalMap(m.getItem)
	if err != nil {
		return nil, err
	}

	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *Mock) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	m.putInput = input
	return &dynamodb.PutItemOutput{}, nil
}
