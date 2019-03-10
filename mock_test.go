package ddb

import (
	"flag"
	"sync"

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
	mutex     sync.Mutex
	err       error
	getItem   interface{}
	scanItems []interface{}
	getInput  *dynamodb.GetItemInput
	putInput  *dynamodb.PutItemInput
	scanInput *dynamodb.ScanInput
}

func (m *Mock) CreateTableWithContext(aws.Context, *dynamodb.CreateTableInput, ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, m.err
}

func (m *Mock) DeleteTableWithContext(aws.Context, *dynamodb.DeleteTableInput, ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return &dynamodb.DeleteTableOutput{}, m.err
}

func (m *Mock) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	m.getInput = input

	var item map[string]*dynamodb.AttributeValue
	if m.getItem != nil {
		v, err := dynamodbattribute.MarshalMap(m.getItem)
		if err != nil {
			return nil, err
		}
		item = v
	}

	return &dynamodb.GetItemOutput{
		Item: item,
		ConsumedCapacity: &dynamodb.ConsumedCapacity{
			ReadCapacityUnits: aws.Float64(1),
		},
	}, m.err
}

func (m *Mock) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	m.putInput = input
	return &dynamodb.PutItemOutput{
		ConsumedCapacity: &dynamodb.ConsumedCapacity{
			WriteCapacityUnits: aws.Float64(1),
		},
	}, m.err
}

func (m *Mock) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	m.scanInput = input

	var output dynamodb.ScanOutput

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if n := len(m.scanItems); n > 0 {
		item, err := dynamodbattribute.MarshalMap(m.scanItems[0])
		if err == nil {
			output.Items = append(output.Items, item)
		}
		if n > 1 {
			output.LastEvaluatedKey = map[string]*dynamodb.AttributeValue{
				"blah": {S: aws.String("blah")},
			}
		}

		m.scanItems = m.scanItems[1:]
	}

	return &output, m.err
}
