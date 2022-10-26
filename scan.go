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
	"encoding/json"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Item provides handle to each record that can be unmarshalled
type Item interface {
	// Raw returns the raw value of the element
	Raw() map[string]*dynamodb.AttributeValue

	// Unmarshal the record into the provided interface
	Unmarshal(v interface{}) error
}

type baseItem struct {
	raw map[string]*dynamodb.AttributeValue
}

// Raw implements Item
func (b baseItem) Raw() map[string]*dynamodb.AttributeValue {
	return b.raw
}

func (b baseItem) Unmarshal(v interface{}) error {
	return dynamodbattribute.UnmarshalMap(b.raw, v)
}

// Scan encapsulates a scan request
type Scan struct {
	api            dynamodbiface.DynamoDBAPI
	spec           *tableSpec
	consistentRead bool
	request        *ConsumedCapacity
	table          *ConsumedCapacity
	debug          io.Writer
	err            error
	expr           *expression
	indexName      string
	totalSegments  int64
}

func (s *Scan) makeScanInput(segment, totalSegments int64, startKey map[string]*dynamodb.AttributeValue) *dynamodb.ScanInput {
	var (
		filterExpr = s.expr.ConditionExpression()
	)

	input := dynamodb.ScanInput{
		ConsistentRead:            aws.Bool(s.consistentRead),
		ExclusiveStartKey:         startKey,
		ExpressionAttributeNames:  s.expr.Names,
		ExpressionAttributeValues: s.expr.Values,
		FilterExpression:          filterExpr,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Segment:                   aws.Int64(segment),
		TableName:                 aws.String(s.spec.TableName),
		TotalSegments:             aws.Int64(s.totalSegments),
	}
	if s.indexName != "" {
		input.IndexName = aws.String(s.indexName)
	}

	return &input
}

func (s *Scan) scanSegment(ctx context.Context, segment, totalSegments int64, fn func(item Item) (bool, error)) (stop bool, err error) {
	var startKey map[string]*dynamodb.AttributeValue

	for {
		input := s.makeScanInput(segment, totalSegments, startKey)
		output, err := s.api.ScanWithContext(ctx, input)
		if err != nil {
			return false, err
		}

		s.table.add(output.ConsumedCapacity)
		if s.request != nil {
			s.request.add(output.ConsumedCapacity)
		}

		var item baseItem
		for _, rawItem := range output.Items {
			item.raw = rawItem
			ok, err := fn(item)
			if err != nil {
				return false, err
			}
			if !ok {
				return true, nil
			}
		}

		startKey = output.LastEvaluatedKey
		if startKey == nil {
			break
		}
	}

	return false, nil
}

// ConsistentRead enables or disables consistent reading
func (s *Scan) ConsistentRead(enabled bool) *Scan {
	s.consistentRead = true
	return s
}

// ConsumedCapacity captures consumed capacity to the property provided
func (s *Scan) ConsumedCapacity(capture *ConsumedCapacity) *Scan {
	s.request = capture
	return s
}

// Debug dynamodb request
func (s *Scan) Debug(w io.Writer) *Scan {
	s.debug = w
	return s
}

// Each is identical to EachWithContext except that it does not allow for cancellation
// via the context.
func (s *Scan) Each(callback func(item Item) (bool, error)) error {
	return s.EachWithContext(defaultContext, callback)
}

// EachWithContext iterates invokes the callback for each record that matches the scan.
// So long as the callback returns `true, nil`, the scan will continue.  If the callback
// either returns an error OR false, the scan will stop.  The scan will also stop if the
// context has been canceled.
func (s *Scan) EachWithContext(ctx context.Context, callback func(item Item) (bool, error)) error {
	if s.err != nil {
		return s.err
	}

	if s.totalSegments == 0 {
		s.totalSegments = 1
	}

	if s.debug != nil {
		input := s.makeScanInput(0, s.totalSegments, nil)
		_ = json.NewEncoder(s.debug).Encode(input)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, s.totalSegments)
	wg := &sync.WaitGroup{}
	wg.Add(int(s.totalSegments))
	for i := s.totalSegments - 1; i >= 0; i-- {
		go func(segment int64) {
			defer wg.Done()

			stop, err := s.scanSegment(ctx, segment, s.totalSegments, callback)
			if err != nil {
				errs <- err
			}
			if stop {
				cancel()
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		return err
	}

	return nil
}

// Filter allows for the scan record to be conditionally filtered
func (s *Scan) Filter(expr string, values ...interface{}) *Scan {
	if err := s.expr.Condition(expr, values...); err != nil {
		s.err = err
	}

	return s
}

// First returns the first scanned record
func (s *Scan) First(v interface{}) error {
	return s.FirstWithContext(defaultContext, v)
}

// FirstWithContext returns the first scanned record and allows for cancellation
func (s *Scan) FirstWithContext(ctx context.Context, v interface{}) error {
	mux := &sync.Mutex{}
	count := 0
	fn := func(item Item) (bool, error) {
		mux.Lock()
		defer mux.Unlock()

		if err := item.Unmarshal(v); err != nil {
			return false, err
		}

		count++
		return false, nil
	}

	if err := s.EachWithContext(ctx, fn); err != nil {
		return err
	}

	if count == 0 {
		return errorf(ErrItemNotFound, "item not found")
	}

	return nil
}

// IndexName to scan for
func (s *Scan) IndexName(indexName string) *Scan {
	s.indexName = indexName
	return s
}

// TotalSegments allows for the Scan operation to run in parallel.  If not set, defaults
// to 1 segment
func (s *Scan) TotalSegments(n int64) *Scan {
	s.totalSegments = n
	return s
}

// Scan initiates the scan operation
func (t *Table) Scan() *Scan {
	return &Scan{
		api:   t.ddb.api,
		table: t.consumed,
		expr:  newExpression(t.spec.Attributes...),
		spec:  t.spec,
	}
}
