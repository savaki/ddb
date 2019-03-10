package ddb

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Item interface {
	Unmarshal(v interface{}) error
}

type baseItem struct {
	raw map[string]*dynamodb.AttributeValue
}

func (b baseItem) Unmarshal(v interface{}) error {
	return dynamodbattribute.UnmarshalMap(b.raw, v)
}

type Scan struct {
	api            dynamodbiface.DynamoDBAPI
	spec           *tableSpec
	consistentRead bool
	totalSegments  int64
	consumed       *ConsumedCapacity
}

func (s *Scan) ConsistentRead(enabled bool) *Scan {
	s.consistentRead = true
	return s
}

func (s *Scan) makeScanInput(segment, totalSegments int64, startKey map[string]*dynamodb.AttributeValue) *dynamodb.ScanInput {
	return &dynamodb.ScanInput{
		ConsistentRead:         aws.Bool(s.consistentRead),
		ExclusiveStartKey:      startKey,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Segment:                aws.Int64(segment),
		TableName:              aws.String(s.spec.TableName),
		TotalSegments:          aws.Int64(s.totalSegments),
	}
}

func (s *Scan) scanSegment(ctx context.Context, segment, totalSegments int64, fn func(item Item) (bool, error)) (stop bool, err error) {
	var startKey map[string]*dynamodb.AttributeValue

	for {
		input := s.makeScanInput(segment, totalSegments, startKey)
		output, err := s.api.ScanWithContext(ctx, input)
		if err != nil {
			return false, err
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

func (s *Scan) EachWithContext(ctx context.Context, fn func(item Item) (bool, error)) error {
	if s.totalSegments == 0 {
		s.totalSegments = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, s.totalSegments)
	wg := &sync.WaitGroup{}
	wg.Add(int(s.totalSegments))
	for i := s.totalSegments - 1; i >= 0; i-- {
		go func(segment int64) {
			defer wg.Done()

			stop, err := s.scanSegment(ctx, segment, s.totalSegments, fn)
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

func (s *Scan) Each(fn func(item Item) (bool, error)) error {
	return s.EachWithContext(defaultContext, fn)
}

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

func (s *Scan) First(v interface{}) error {
	return s.FirstWithContext(defaultContext, v)
}

func (t *Table) Scan() *Scan {
	return &Scan{
		api:      t.ddb.api,
		spec:     t.spec,
		consumed: t.consumed,
	}
}
