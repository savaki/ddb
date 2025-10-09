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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

const (
	// DefaultBillingMode specifies the default billing mode for DynamoDB tables as provisioned capacity
	DefaultBillingMode = types.BillingModeProvisioned
	// DefaultReadCapacity specifies the default read capacity units for provisioned tables
	DefaultReadCapacity = int64(3)
	// DefaultWriteCapacity specifies the default write capacity units for provisioned tables
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
	billingMode        types.BillingMode
	projectionType     string
	readCapacityUnits  int64
	streamViewType     string
	writeCapacityUnits int64
}

// TableOption defines an interface for applying configuration options to table creation
type TableOption interface {
	ApplyTable(o *tableOptions)
}

// TableIndexOption defines an interface for options that can be applied to both tables and indexes
type TableIndexOption interface {
	TableOption
}

type tableIndexFunc func(o *tableOptions)

func (fn tableIndexFunc) ApplyTable(o *tableOptions) {
	fn(o)
}

// WithBillingMode configures the billing mode for a DynamoDB table.
// Common modes are types.BillingModeProvisioned and types.BillingModePayPerRequest.
func WithBillingMode(mode types.BillingMode) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.billingMode = mode
	})
}

// WithReadCapacity sets the read capacity units for provisioned tables and indexes.
// This setting is only used when billing mode is set to provisioned capacity.
func WithReadCapacity(rcap int64) TableIndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.readCapacityUnits = rcap
	})
}

// WithStreamSpecification enables DynamoDB streams for the table with the specified view type.
// Valid values include KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, and NEW_AND_OLD_IMAGES.
func WithStreamSpecification(streamViewType string) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.streamViewType = streamViewType
	})
}

// WithWriteCapacity sets the write capacity units for provisioned tables and indexes.
// This setting is only used when billing mode is set to provisioned capacity.
func WithWriteCapacity(wcap int64) TableIndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.writeCapacityUnits = wcap
	})
}

func makeAttributeDefinitions(spec *tableSpec) []types.AttributeDefinition {
	var (
		items []types.AttributeDefinition
		seen  = map[string]struct{}{}
	)

	addKey := func(item *keySpec) {
		if item == nil {
			return
		}
		if _, ok := seen[item.AttributeName]; ok {
			return
		}
		items = append(items, types.AttributeDefinition{
			AttributeName: &item.AttributeName,
			AttributeType: types.ScalarAttributeType(item.AttributeType),
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
		items = append(items, types.AttributeDefinition{
			AttributeName: &item.AttributeName,
			AttributeType: types.ScalarAttributeType(item.AttributeType),
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

func makeKeySchemaElements(hashKey, rangeKey *keySpec) []types.KeySchemaElement {
	var items []types.KeySchemaElement
	if hashKey != nil {
		items = append(items, types.KeySchemaElement{
			AttributeName: &hashKey.AttributeName,
			KeyType:       types.KeyTypeHash,
		})
	}
	if rangeKey != nil {
		items = append(items, types.KeySchemaElement{
			AttributeName: &rangeKey.AttributeName,
			KeyType:       types.KeyTypeRange,
		})
	}
	return items
}

func makeProvisionedThroughput(options tableOptions) *types.ProvisionedThroughput {
	if options.billingMode == types.BillingModePayPerRequest {
		return nil
	}

	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  &options.readCapacityUnits,
		WriteCapacityUnits: &options.writeCapacityUnits,
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

	billingMode := options.billingMode
	streamEnabled := true
	input := dynamodb.CreateTableInput{
		AttributeDefinitions:  makeAttributeDefinitions(spec),
		BillingMode:           billingMode,
		KeySchema:             makeKeySchemaElements(spec.HashKey, spec.RangeKey),
		ProvisionedThroughput: makeProvisionedThroughput(options),
		TableName:             &tableName,
	}
	if options.streamViewType != "" {
		input.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  &streamEnabled,
			StreamViewType: types.StreamViewType(options.streamViewType),
		}
	}

	for _, item := range spec.Locals {
		lsi := types.LocalSecondaryIndex{
			IndexName: &item.IndexName,
			KeySchema: makeKeySchemaElements(item.HashKey, item.RangeKey),
		}
		if len(item.Attributes) == 0 {
			if item.KeysOnly {
				lsi.Projection = &types.Projection{
					ProjectionType: types.ProjectionTypeKeysOnly,
				}
			} else {
				lsi.Projection = &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				}
			}
		} else {
			lsi.Projection = &types.Projection{
				ProjectionType: types.ProjectionTypeInclude,
			}
			for _, attr := range item.Attributes {
				lsi.Projection.NonKeyAttributes = append(lsi.Projection.NonKeyAttributes, attr.AttributeName)
			}
		}

		input.LocalSecondaryIndexes = append(input.LocalSecondaryIndexes, lsi)
	}

	for _, item := range spec.Globals {
		gsi := types.GlobalSecondaryIndex{
			IndexName: &item.IndexName,
			KeySchema: makeKeySchemaElements(item.HashKey, item.RangeKey),
		}
		if options.billingMode == types.BillingModeProvisioned {
			gsi.ProvisionedThroughput = &types.ProvisionedThroughput{
				ReadCapacityUnits:  &options.readCapacityUnits,
				WriteCapacityUnits: &options.writeCapacityUnits,
			}
		}
		if len(item.Attributes) == 0 {
			if item.KeysOnly {
				gsi.Projection = &types.Projection{
					ProjectionType: types.ProjectionTypeKeysOnly,
				}
			} else {
				gsi.Projection = &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				}
			}
		} else {
			gsi.Projection = &types.Projection{
				ProjectionType: types.ProjectionTypeInclude,
			}
			for _, attr := range item.Attributes {
				gsi.Projection.NonKeyAttributes = append(gsi.Projection.NonKeyAttributes, attr.AttributeName)
			}
		}

		input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, gsi)
	}

	return input
}

// CreateTableIfNotExists creates a DynamoDB table if it does not already exist.
// If the table already exists, this method returns nil without error.
// Options can be provided to configure billing mode, capacity, and stream settings.
func (t *Table) CreateTableIfNotExists(ctx context.Context, opts ...TableOption) error {
	input := makeCreateTableInput(t.tableName, t.spec, opts...)
	if _, err := t.ddb.api.CreateTable(ctx, &input); err != nil {
		var apiErr smithy.APIError
		if ok := errors.As(err, &apiErr); ok && apiErr.ErrorCode() == "ResourceInUseException" {
			return nil
		}
		return err
	}

	return nil
}

// DeleteTableIfExists deletes a DynamoDB table if it exists.
// If the table does not exist, this method returns nil without error.
func (t *Table) DeleteTableIfExists(ctx context.Context) error {
	input := dynamodb.DeleteTableInput{
		TableName: &t.tableName,
	}
	if _, err := t.ddb.api.DeleteTable(ctx, &input); err != nil {
		var apiErr smithy.APIError
		if ok := errors.As(err, &apiErr); ok && apiErr.ErrorCode() == "ResourceNotFoundException" {
			return nil
		}

		return err
	}

	return nil
}
