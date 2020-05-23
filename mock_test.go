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
	mutex      sync.Mutex
	err        error
	getItem    interface{}
	queryItems []interface{}
	scanItems  []interface{}
	updateItem interface{}
	readUnits  int64 // readUnits capacity to return
	writeUnits int64 // writeUnits capacity to return

	deleteInput *dynamodb.DeleteItemInput
	getInput    *dynamodb.GetItemInput
	putInput    *dynamodb.PutItemInput
	queryInput  *dynamodb.QueryInput
	scanInput   *dynamodb.ScanInput
	updateInput *dynamodb.UpdateItemInput
	writeInput  *dynamodb.TransactWriteItemsInput
}

func (m *Mock) CreateTableWithContext(aws.Context, *dynamodb.CreateTableInput, ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, m.err
}

func (m *Mock) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	m.deleteInput = input

	return &dynamodb.DeleteItemOutput{
		ConsumedCapacity: &dynamodb.ConsumedCapacity{
			ReadCapacityUnits:  aws.Float64(float64(m.readUnits)),
			WriteCapacityUnits: aws.Float64(float64(m.writeUnits)),
		},
	}, m.err
}

func (m *Mock) DeleteTableWithContext(aws.Context, *dynamodb.DeleteTableInput, ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return &dynamodb.DeleteTableOutput{}, m.err
}

func (m *Mock) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	m.getInput = input

	var item map[string]*dynamodb.AttributeValue
	if m.getItem != nil {
		v, err := marshalMap(m.getItem)
		if err != nil {
			return nil, err
		}
		item = v
	}

	return &dynamodb.GetItemOutput{
		Item: item,
		ConsumedCapacity: &dynamodb.ConsumedCapacity{
			ReadCapacityUnits:  aws.Float64(float64(m.readUnits)),
			WriteCapacityUnits: aws.Float64(float64(m.writeUnits)),
		},
	}, m.err
}

func (m *Mock) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	m.putInput = input
	return &dynamodb.PutItemOutput{
		ConsumedCapacity: &dynamodb.ConsumedCapacity{
			ReadCapacityUnits:  aws.Float64(float64(m.readUnits)),
			WriteCapacityUnits: aws.Float64(float64(m.writeUnits)),
		},
	}, m.err
}

func (m *Mock) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	m.queryInput = input
	output := dynamodb.QueryOutput{
		ConsumedCapacity: &dynamodb.ConsumedCapacity{
			ReadCapacityUnits:  aws.Float64(float64(m.readUnits)),
			WriteCapacityUnits: aws.Float64(float64(m.writeUnits)),
		},
	}

	for _, item := range m.queryItems {
		v, err := marshalMap(item)
		if err != nil {
			return nil, err
		}

		output.Items = append(output.Items, v)
	}

	return &output, m.err
}

func (m *Mock) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	m.scanInput = input

	var output dynamodb.ScanOutput

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if n := len(m.scanItems); n > 0 {
		item, err := marshalMap(m.scanItems[0])
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

	output.ConsumedCapacity = &dynamodb.ConsumedCapacity{
		ReadCapacityUnits:  aws.Float64(float64(m.readUnits)),
		WriteCapacityUnits: aws.Float64(float64(m.writeUnits)),
	}

	return &output, m.err
}

func (m *Mock) TransactWriteItemsWithContext(ctx aws.Context, input *dynamodb.TransactWriteItemsInput, opts ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	m.writeInput = input
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (m *Mock) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	m.updateInput = input

	output := dynamodb.UpdateItemOutput{
		ConsumedCapacity: &dynamodb.ConsumedCapacity{
			ReadCapacityUnits:  aws.Float64(float64(m.readUnits)),
			WriteCapacityUnits: aws.Float64(float64(m.writeUnits)),
		},
	}
	if m.updateItem != nil {
		item, err := dynamodbattribute.MarshalMap(m.updateItem)
		if err != nil {
			return nil, err
		}
		output.Attributes = item
	}

	return &output, m.err
}
