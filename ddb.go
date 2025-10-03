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
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/segmentio/ksuid"
)

const (
	defaultMaxAttempts = 4                      // defaultMaxAttempts holds default max attempts for Transact* ops
	defaultTimeout     = 100 * time.Millisecond // defaultTimeout holds initial timeout between Transact* attempts
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

func (c *ConsumedCapacity) add(in *types.ConsumedCapacity) {
	if in == nil {
		return
	}
	if in.ReadCapacityUnits != nil && *in.ReadCapacityUnits > 0 {
		atomic.AddInt64(&c.ReadUnits, int64(*in.ReadCapacityUnits))
	}
	if in.WriteCapacityUnits != nil && *in.WriteCapacityUnits > 0 {
		atomic.AddInt64(&c.WriteUnits, int64(*in.WriteCapacityUnits))
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

// DynamoDBAPI defines the interface for DynamoDB operations
type DynamoDBAPI interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DeleteTable(ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

type DDB struct {
	api        DynamoDBAPI
	tokenFunc  func() string
	txAttempts int                     // txAttempts refers to max number of times an Transact* will be attempted
	txTimeout  func(int) time.Duration // txTimeout provides the getTimeout given a duration
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

// WithTokenFunc allows the generator func for dynamodb transactions to be overwritten
func (d *DDB) WithTokenFunc(fn func() string) *DDB {
	if fn == nil {
		fn = makeRequestToken
	}
	d.tokenFunc = fn
	return d
}

// WithTransactAttempts overrides the number of times to attempt a Transact before
// giving up.  Defaults to 4
func (d *DDB) WithTransactAttempts(n int) *DDB {
	if n < 0 || n >= 10 {
		panic(fmt.Errorf("WithTransactAttempts requires 0 < n < 10: got %v", n))
	}
	return &DDB{
		api:        d.api,
		tokenFunc:  d.tokenFunc,
		txAttempts: n,
		txTimeout:  d.txTimeout,
	}
}

// WithTransactTimeout allows the timeout progression to be customized.  By default
// uses exponential backoff e.g. attempt^2 * duration
func (d *DDB) WithTransactTimeout(fn func(i int) time.Duration) *DDB {
	if fn == nil {
		fn = getTimeout
	}
	return &DDB{
		api:        d.api,
		tokenFunc:  d.tokenFunc,
		txAttempts: d.txAttempts,
		txTimeout:  fn,
	}
}

// GetTx encapsulates a transactional get operation
type GetTx interface {
	// Decode the response from AWS
	Decode(v *types.ItemResponse) error
	// Tx generates the get input
	Tx() (*types.TransactGetItem, error)
}

// TransactGetItemsWithContext wraps the get operations using a TransactGetItems
func (d *DDB) TransactGetItemsWithContext(ctx context.Context, gets ...GetTx) (err error) {
	input := dynamodb.TransactGetItemsInput{
		TransactItems: make([]types.TransactGetItem, 0, len(gets)),
	}
	for _, get := range gets {
		v, err := get.Tx()
		if err != nil {
			return err
		}
		input.TransactItems = append(input.TransactItems, *v)
	}

	var e error

loop:
	for attempt := 1; attempt <= d.txAttempts; attempt++ {
		output, err := d.api.TransactGetItems(ctx, &input)
		if err != nil {
			var tce *types.TransactionCanceledException
			if ok := errors.As(err, &tce); ok {
				for _, reason := range tce.CancellationReasons {
					if reason.Code != nil && *reason.Code == "TransactionConflict" {
						timeout := d.txTimeout(attempt)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(timeout):
							e = err
							continue loop
						}
					}
				}
			}
			return err
		}

		for i, item := range output.Responses {
			get := gets[i]
			if err := get.Decode(&item); err != nil {
				return err
			}
		}

		return nil
	}

	return e
}

// TransactGetItems allows TransactGetItems to be called without a context
func (d *DDB) TransactGetItems(items ...GetTx) error {
	return d.TransactGetItemsWithContext(defaultContext, items...)
}

// WriteTx converts ddb operations into instances of *types.TransactWriteItem
type WriteTx interface {
	Tx() (*types.TransactWriteItem, error)
}

// TransactWriteItemsWithContext applies the provided operations in a dynamodb transaction.
// Subject to the limits of of TransactWriteItems.
func (d *DDB) TransactWriteItemsWithContext(ctx context.Context, items ...WriteTx) (*dynamodb.TransactWriteItemsOutput, error) {
	token := d.tokenFunc()
	input := dynamodb.TransactWriteItemsInput{
		ClientRequestToken: &token,
		TransactItems:      make([]types.TransactWriteItem, 0, len(items)),
	}

	for _, item := range items {
		v, err := item.Tx()
		if err != nil {
			return nil, err
		}
		input.TransactItems = append(input.TransactItems, *v)
	}

	var e error

loop:
	for attempt := 1; attempt <= d.txAttempts; attempt++ {
		output, err := d.api.TransactWriteItems(ctx, &input)
		if err != nil {
			var tce *types.TransactionCanceledException
			if ok := errors.As(err, &tce); ok {
				for _, reason := range tce.CancellationReasons {
					if reason.Code != nil {
						code := *reason.Code
						if code == "TransactionConflictException" || code == "TransactionConflict" {
							timeout := d.txTimeout(attempt)
							select {
							case <-ctx.Done():
								return nil, ctx.Err()
							case <-time.After(timeout):
								e = err
								continue loop
							}
						}
					}
				}
			}
			return nil, err
		}

		return output, nil
	}

	return nil, e
}

func (d *DDB) TransactWriteItems(items ...WriteTx) (*dynamodb.TransactWriteItemsOutput, error) {
	return d.TransactWriteItemsWithContext(defaultContext, items...)
}

func New(api DynamoDBAPI) *DDB {
	return &DDB{
		api:        api,
		tokenFunc:  makeRequestToken,
		txAttempts: defaultMaxAttempts,
		txTimeout:  getTimeout,
	}
}

// getTimeout returns a timeout equal to attempt^2*defaultTimeout e.g. exponential backoff
func getTimeout(attempt int) time.Duration {
	d := defaultTimeout
	for i := 0; i < attempt; i++ {
		d *= 2
	}
	return d
}

func makeRequestToken() string {
	return ksuid.New().String()
}
