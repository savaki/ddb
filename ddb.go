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
	"encoding/binary"
	"errors"
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
	api        dynamodbiface.DynamoDBAPI
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
	Decode(v *dynamodb.ItemResponse) error
	// Tx generates the get input
	Tx() (*dynamodb.TransactGetItem, error)
}

// TransactGetItemsWithContext wraps the get operations using a TransactGetItems
func (d *DDB) TransactGetItemsWithContext(ctx context.Context, gets ...GetTx) (err error) {
	input := dynamodb.TransactGetItemsInput{
		TransactItems: make([]*dynamodb.TransactGetItem, 0, len(gets)),
	}
	for _, get := range gets {
		v, err := get.Tx()
		if err != nil {
			return err
		}
		input.TransactItems = append(input.TransactItems, v)
	}

	var e error

loop:
	for attempt := 1; attempt <= d.txAttempts; attempt++ {
		output, err := d.api.TransactGetItemsWithContext(ctx, &input)
		if err != nil {
			var tce *dynamodb.TransactionCanceledException
			if ok := errors.As(err, &tce); ok {
				for _, reason := range tce.CancellationReasons {
					if aws.StringValue(reason.Code) == "TransactionConflict" {
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
			if err := get.Decode(item); err != nil {
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
		v, err := item.Tx()
		if err != nil {
			return nil, err
		}
		input.TransactItems = append(input.TransactItems, v)
	}

	var e error

loop:
	for attempt := 1; attempt <= d.txAttempts; attempt++ {
		output, err := d.api.TransactWriteItemsWithContext(ctx, &input)
		if err != nil {
			var tce *dynamodb.TransactionCanceledException
			if ok := errors.As(err, &tce); ok {
				for _, reason := range tce.CancellationReasons {
					if code := aws.StringValue(reason.Code); code == "TransactionConflictException" || code == "TransactionConflict" {
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
			return nil, err
		}

		return output, nil
	}

	return nil, e
}

func (d *DDB) TransactWriteItems(items ...WriteTx) (*dynamodb.TransactWriteItemsOutput, error) {
	return d.TransactWriteItemsWithContext(defaultContext, items...)
}

func New(api dynamodbiface.DynamoDBAPI) *DDB {
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
	var token [12]byte
	r.Read(token[:])

	var (
		now = time.Now().UnixNano() / int64(time.Microsecond)
		a   = binary.BigEndian.Uint64(token[0:8])
		b   = binary.BigEndian.Uint32(token[8:])
	)

	return strconv.FormatInt(now, 36) + strconv.FormatUint(a, 36) + strconv.FormatUint(uint64(b), 36)
}
