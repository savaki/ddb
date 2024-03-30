package ddbtest

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/savaki/ddb"
)

// EventBuilder defines a minimal implementation of a ddb.Event
type EventBuilder struct {
	fns []func() (ddb.Record, error)
}

// New creates a new EventBuilder
func New() *EventBuilder {
	return &EventBuilder{}
}

func (b *EventBuilder) Insert(newItem interface{}) *EventBuilder {
	fn := func() (ddb.Record, error) {
		newImage, err := dynamodbattribute.MarshalMap(newItem)
		if err != nil {
			return ddb.Record{}, err
		}

		return ddb.Record{
			Change: ddb.Change{
				NewImage: newImage,
			},
			EventName: dynamodbstreams.OperationTypeInsert,
		}, nil
	}

	b.fns = append(b.fns, fn)

	return b
}

func (b *EventBuilder) Modify(oldItem, newItem interface{}) *EventBuilder {
	fn := func() (ddb.Record, error) {
		newImage, err := dynamodbattribute.MarshalMap(newItem)
		if err != nil {
			return ddb.Record{}, err
		}

		oldImage, err := dynamodbattribute.MarshalMap(oldItem)
		if err != nil {
			return ddb.Record{}, err
		}

		return ddb.Record{
			Change: ddb.Change{
				NewImage: newImage,
				OldImage: oldImage,
			},
			EventName: dynamodbstreams.OperationTypeModify,
		}, nil
	}

	b.fns = append(b.fns, fn)

	return b
}

func (b *EventBuilder) Remove(oldItem interface{}) *EventBuilder {
	fn := func() (ddb.Record, error) {
		oldImage, err := dynamodbattribute.MarshalMap(oldItem)
		if err != nil {
			return ddb.Record{}, err
		}

		return ddb.Record{
			Change: ddb.Change{
				OldImage: oldImage,
			},
			EventName: dynamodbstreams.OperationTypeRemove,
		}, nil
	}

	b.fns = append(b.fns, fn)

	return b
}

func (b *EventBuilder) Build() (event ddb.Event, err error) {
	for _, fn := range b.fns {
		record, err := fn()
		if err != nil {
			return ddb.Event{}, err
		}
		event.Records = append(event.Records, record)
	}
	return event, nil
}
