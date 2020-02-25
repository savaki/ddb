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
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	defaultContext = context.Background()
	r              = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type ConsumedCapacity struct {
	mux           sync.Mutex
	capacityUnits float64
	ReadUnits     int64
	WriteUnits    int64
}

func (c *ConsumedCapacity) CapacityUnits() float64 {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.capacityUnits
}

func (c *ConsumedCapacity) add(in *dynamodb.ConsumedCapacity) {
	if in == nil {
		return
	}
	if units := in.ReadCapacityUnits; units != nil && *units > 0 {
		atomic.AddInt64(&c.ReadUnits, int64(*units))
	}
	if units := in.WriteCapacityUnits; units != nil && *units > 0 {
		atomic.AddInt64(&c.WriteUnits, int64(*units))
	}

	if in.CapacityUnits != nil {
		c.mux.Lock()
		c.capacityUnits += *in.CapacityUnits
		c.mux.Unlock()
	}
}

func (c *ConsumedCapacity) safeClone() ConsumedCapacity {
	c.mux.Lock()
	defer c.mux.Unlock()

	return ConsumedCapacity{
		ReadUnits:     atomic.LoadInt64(&c.ReadUnits),
		WriteUnits:    atomic.LoadInt64(&c.WriteUnits),
		capacityUnits: c.capacityUnits,
	}
}

type Table struct {
	ddb       *DDB
	spec      *tableSpec
	tableName string
	consumed  *ConsumedCapacity
}

func (t *Table) ConsumedCapacity() ConsumedCapacity {
	return t.consumed.safeClone()
}

func (t *Table) DDB() *DDB {
	return t.ddb
}

type DDB struct {
	api       dynamodbiface.DynamoDBAPI
	tokenFunc func() string
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
		consumed:  &ConsumedCapacity{},
	}, nil
}

func (d *DDB) MustTable(tableName string, model interface{}) *Table {
	table, err := d.Table(tableName, model)
	if err != nil {
		panic(err)
	}
	return table
}

// WriteTx converts ddb operations into instances of *dynamodb.TransactWriteItem
type WriteTx interface {
	Tx() (*dynamodb.TransactWriteItem, error)
}

// TransactWriteItemsWithContext applies the provided operations in a dynamodb transaction.
// Subject to the limits of of TransactWriteItems.
func (d *DDB) TransactWriteItemsWithContext(ctx context.Context, items ...WriteTx) (*dynamodb.TransactWriteItemsOutput, error) {
	token := d.tokenFunc()
	input := dynamodb.TransactWriteItemsInput{
		ClientRequestToken: aws.String(token),
		TransactItems:      make([]*dynamodb.TransactWriteItem, 0, len(items)),
	}

	for _, item := range items {
		i, err := item.Tx()
		if err != nil {
			return nil, err
		}
		input.TransactItems = append(input.TransactItems, i)
	}

	return d.api.TransactWriteItemsWithContext(ctx, &input)
}

func (d *DDB) TransactWriteItems(items ...WriteTx) (*dynamodb.TransactWriteItemsOutput, error) {
	return d.TransactWriteItemsWithContext(defaultContext, items...)
}

func New(api dynamodbiface.DynamoDBAPI) *DDB {
	return &DDB{
		api:       api,
		tokenFunc: makeRequestToken,
	}
}

func makeRequestToken() string {
	var token [12]byte
	r.Read(token[:])

	var (
		now = time.Now().UnixNano() / int64(time.Microsecond)
		a   = binary.BigEndian.Uint64(token[0:8])
		b   = binary.BigEndian.Uint32(token[8:])
	)

	return strconv.FormatInt(now, 36) + strconv.FormatUint(a, 36) + strconv.FormatUint(uint64(b), 36)
}
