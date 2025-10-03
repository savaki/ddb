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
	"flag"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

var runIntegrationTests bool

func init() {
	flag.BoolVar(&runIntegrationTests, "integration", false, "run integration tests")
}

type Mock struct {
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

func (m *Mock) CreateTable(ctx context.Context, input *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, m.err
}

func (m *Mock) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.deleteInput = input

	readUnits := float64(m.readUnits)
	writeUnits := float64(m.writeUnits)
	return &dynamodb.DeleteItemOutput{
		ConsumedCapacity: &types.ConsumedCapacity{
			ReadCapacityUnits:  &readUnits,
			WriteCapacityUnits: &writeUnits,
		},
	}, m.err
}

func (m *Mock) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	return &dynamodb.DeleteTableOutput{}, m.err
}

func (m *Mock) DescribeTable(ctx context.Context, input *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return &dynamodb.DescribeTableOutput{}, m.err
}

func (m *Mock) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.getInput = input

	var item map[string]types.AttributeValue
	if m.getItem != nil {
		v, err := marshalMap(m.getItem)
		if err != nil {
			return nil, err
		}
		item = v
	}

	readUnits := float64(m.readUnits)
	writeUnits := float64(m.writeUnits)
	return &dynamodb.GetItemOutput{
		Item: item,
		ConsumedCapacity: &types.ConsumedCapacity{
			ReadCapacityUnits:  &readUnits,
			WriteCapacityUnits: &writeUnits,
		},
	}, m.err
}

func (m *Mock) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.putInput = input
	readUnits := float64(m.readUnits)
	writeUnits := float64(m.writeUnits)
	return &dynamodb.PutItemOutput{
		ConsumedCapacity: &types.ConsumedCapacity{
			ReadCapacityUnits:  &readUnits,
			WriteCapacityUnits: &writeUnits,
		},
	}, m.err
}

func (m *Mock) Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.queryInput = input
	readUnits := float64(m.readUnits)
	writeUnits := float64(m.writeUnits)
	output := dynamodb.QueryOutput{
		ConsumedCapacity: &types.ConsumedCapacity{
			ReadCapacityUnits:  &readUnits,
			WriteCapacityUnits: &writeUnits,
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

func (m *Mock) Scan(ctx context.Context, input *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
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
			s := "blah"
			output.LastEvaluatedKey = map[string]types.AttributeValue{
				"blah": &types.AttributeValueMemberS{Value: s},
			}
		}

		m.scanItems = m.scanItems[1:]
	}

	readUnits := float64(m.readUnits)
	writeUnits := float64(m.writeUnits)
	output.ConsumedCapacity = &types.ConsumedCapacity{
		ReadCapacityUnits:  &readUnits,
		WriteCapacityUnits: &writeUnits,
	}

	return &output, m.err
}

func (m *Mock) TransactGetItems(ctx context.Context, input *dynamodb.TransactGetItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	return &dynamodb.TransactGetItemsOutput{}, m.err
}

func (m *Mock) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	m.writeInput = input
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (m *Mock) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.updateInput = input

	readUnits := float64(m.readUnits)
	writeUnits := float64(m.writeUnits)
	output := dynamodb.UpdateItemOutput{
		ConsumedCapacity: &types.ConsumedCapacity{
			ReadCapacityUnits:  &readUnits,
			WriteCapacityUnits: &writeUnits,
		},
	}
	if m.updateItem != nil {
		item, err := attributevalue.MarshalMap(m.updateItem)
		if err != nil {
			return nil, err
		}
		output.Attributes = item
	}

	return &output, m.err
}
