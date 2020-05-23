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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	DefaultBillingMode   = dynamodb.BillingModeProvisioned
	DefaultReadCapacity  = int64(3)
	DefaultWriteCapacity = int64(3)
)

type keyOptions struct {
	hashKey  *keySpec
	rangeKey *keySpec
}

type attribute struct {
	Name string
	Type string
}

type tableOptions struct {
	attributes         []attribute
	keys               keyOptions
	billingMode        string
	projectionType     string
	readCapacityUnits  int64
	streamViewType     string
	writeCapacityUnits int64
}

type TableOption interface {
	ApplyTable(o *tableOptions)
}

type TableIndexOption interface {
	TableOption
}

type tableIndexFunc func(o *tableOptions)

func (fn tableIndexFunc) ApplyTable(o *tableOptions) {
	fn(o)
}

func WithBillingMode(mode string) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.billingMode = mode
	})
}

func WithReadCapacity(rcap int64) TableIndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.readCapacityUnits = rcap
	})
}

func WithStreamSpecification(streamViewType string) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.streamViewType = streamViewType
	})
}

func WithWriteCapacity(wcap int64) TableIndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.writeCapacityUnits = wcap
	})
}

func makeAttributeDefinitions(spec *tableSpec) []*dynamodb.AttributeDefinition {
	var (
		items []*dynamodb.AttributeDefinition
		seen  = map[string]struct{}{}
	)

	addKey := func(item *keySpec) {
		if item == nil {
			return
		}
		if _, ok := seen[item.AttributeName]; ok {
			return
		}
		items = append(items, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(item.AttributeName),
			AttributeType: aws.String(item.AttributeType),
		})
		seen[item.AttributeName] = struct{}{}
	}

	addAttr := func(item *attributeSpec) {
		if item == nil {
			return
		}
		if _, ok := seen[item.AttributeName]; ok {
			return
		}
		items = append(items, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(item.AttributeName),
			AttributeType: aws.String(item.AttributeType),
		})
		seen[item.AttributeName] = struct{}{}
	}

	addKey(spec.HashKey)
	addKey(spec.RangeKey)

	for _, m := range [][]*indexSpec{spec.Globals, spec.Locals} {
		for _, index := range m {
			addKey(index.HashKey)
			addKey(index.RangeKey)

			for _, attr := range index.Attributes {
				addAttr(attr)
			}
		}
	}

	return items
}

func makeKeySchemaElements(hashKey, rangeKey *keySpec) []*dynamodb.KeySchemaElement {
	var items []*dynamodb.KeySchemaElement
	if hashKey != nil {
		items = append(items, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(hashKey.AttributeName),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		})
	}
	if rangeKey != nil {
		items = append(items, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(rangeKey.AttributeName),
			KeyType:       aws.String(dynamodb.KeyTypeRange),
		})
	}
	return items
}

func makeProvisionedThroughput(options tableOptions) *dynamodb.ProvisionedThroughput {
	if options.billingMode == dynamodb.BillingModePayPerRequest {
		return nil
	}

	return &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(options.readCapacityUnits),
		WriteCapacityUnits: aws.Int64(options.writeCapacityUnits),
	}
}

func makeTableOptions(opts interface{}) tableOptions {
	options := tableOptions{
		billingMode:        DefaultBillingMode,
		readCapacityUnits:  DefaultReadCapacity,
		writeCapacityUnits: DefaultWriteCapacity,
	}

	switch v := opts.(type) {
	case []TableOption:
		for _, opt := range v {
			opt.ApplyTable(&options)
		}
	}

	return options
}

func makeCreateTableInput(tableName string, spec *tableSpec, opts ...TableOption) dynamodb.CreateTableInput {
	options := makeTableOptions(opts)

	input := dynamodb.CreateTableInput{
		AttributeDefinitions:  makeAttributeDefinitions(spec),
		BillingMode:           aws.String(options.billingMode),
		KeySchema:             makeKeySchemaElements(spec.HashKey, spec.RangeKey),
		ProvisionedThroughput: makeProvisionedThroughput(options),
		TableName:             aws.String(tableName),
	}
	if options.streamViewType != "" {
		input.StreamSpecification = &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(options.streamViewType),
		}
	}

	for _, item := range spec.Locals {
		lsi := dynamodb.LocalSecondaryIndex{
			IndexName: aws.String(item.IndexName),
			KeySchema: makeKeySchemaElements(item.HashKey, item.RangeKey),
		}
		if len(item.Attributes) == 0 {
			if item.KeysOnly {
				lsi.Projection = &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeKeysOnly),
				}
			} else {
				lsi.Projection = &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
				}
			}
		} else {
			lsi.Projection = &dynamodb.Projection{
				ProjectionType: aws.String(dynamodb.ProjectionTypeInclude),
			}
			for _, attr := range item.Attributes {
				lsi.Projection.NonKeyAttributes = append(lsi.Projection.NonKeyAttributes, aws.String(attr.AttributeName))
			}
		}

		input.LocalSecondaryIndexes = append(input.LocalSecondaryIndexes, &lsi)
	}

	for _, item := range spec.Globals {
		gsi := dynamodb.GlobalSecondaryIndex{
			IndexName: aws.String(item.IndexName),
			KeySchema: makeKeySchemaElements(item.HashKey, item.RangeKey),
		}
		if options.billingMode == dynamodb.BillingModeProvisioned {
			gsi.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(options.readCapacityUnits),
				WriteCapacityUnits: aws.Int64(options.writeCapacityUnits),
			}
		}
		if len(item.Attributes) == 0 {
			if item.KeysOnly {
				gsi.Projection = &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeKeysOnly),
				}
			} else {
				gsi.Projection = &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
				}
			}
		} else {
			gsi.Projection = &dynamodb.Projection{
				ProjectionType: aws.String(dynamodb.ProjectionTypeInclude),
			}
			for _, attr := range item.Attributes {
				gsi.Projection.NonKeyAttributes = append(gsi.Projection.NonKeyAttributes, aws.String(attr.AttributeName))
			}
		}

		input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, &gsi)
	}

	return input
}

func (t *Table) CreateTableIfNotExists(ctx context.Context, opts ...TableOption) error {
	input := makeCreateTableInput(t.tableName, t.spec, opts...)
	if _, err := t.ddb.api.CreateTableWithContext(ctx, &input); err != nil {
		if v, ok := err.(awserr.Error); ok && v.Code() == dynamodb.ErrCodeResourceInUseException {
			return nil
		}
		return err
	}

	return nil
}

func (t *Table) DeleteTableIfExists(ctx context.Context) error {
	input := dynamodb.DeleteTableInput{
		TableName: aws.String(t.tableName),
	}
	if _, err := t.ddb.api.DeleteTableWithContext(ctx, &input); err != nil {
		if v, ok := err.(awserr.Error); ok && v.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return nil
		}

		return err
	}

	return nil
}
