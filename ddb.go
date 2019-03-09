// Copyright 2019 Matt Ho
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
	"fmt"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var defaultContext = context.Background()

type consumedCapacity struct {
	readUnits  int64
	writeUnits int64
}

func (c *consumedCapacity) add(in *dynamodb.ConsumedCapacity) {
	if in == nil {
		return
	}
	if rcu := in.ReadCapacityUnits; rcu != nil && *rcu > 0 {
		atomic.AddInt64(&c.readUnits, int64(*rcu))
	}
	if wcu := in.WriteCapacityUnits; wcu != nil && *wcu > 0 {
		atomic.AddInt64(&c.writeUnits, int64(*wcu))
	}
}

type Table struct {
	ddb       *DDB
	spec      *tableSpec
	tableName string
	consumed  *consumedCapacity
}

func (t *Table) DDB() *DDB {
	return t.ddb
}

type DDB struct {
	api dynamodbiface.DynamoDBAPI
}

func (d *DDB) Table(tableName string, model interface{}) (*Table, error) {
	spec, err := inspect(tableName, model)
	if err != nil {
		return nil, fmt.Errorf("unable to create Table: %v", err)
	}

	return &Table{
		ddb:       d,
		spec:      spec,
		tableName: tableName,
		consumed:  &consumedCapacity{},
	}, nil
}

func (d *DDB) MustTable(tableName string, model interface{}) *Table {
	table, err := d.Table(tableName, model)
	if err != nil {
		panic(err)
	}
	return table
}

func New(api dynamodbiface.DynamoDBAPI) *DDB {
	return &DDB{
		api: api,
	}
}
