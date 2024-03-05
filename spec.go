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
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	tagKey       = "ddb"
	tagSeparator = ";"
)

const (
	tagHashKey  = "hash"
	tagRangeKey = "range"
	tagGsiHash  = "gsi_hash:"
	tagGsiRange = "gsi_range:"
	tagGsi      = "gsi:"
	tagLsiRange = "lsi_range:"
	tagLsi      = "lsi:"
)

const (
	optionKeysOnly = "keys_only"
)

type keySpec struct {
	AttributeName string
	AttributeType string
}

type attributeSpec struct {
	FieldName     string // FieldName from struct
	AttributeName string // AttributeName contains dynamodb attribute name
	AttributeType string // AttributeType holds dynamodb type e.g. S, N, B ...
}

type indexSpec struct {
	IndexName  string
	HashKey    *keySpec // hashKey is only used by gsi
	RangeKey   *keySpec
	Attributes []*attributeSpec
	KeysOnly   bool
}

type tableSpec struct {
	TableName  string
	HashKey    *keySpec
	RangeKey   *keySpec
	Attributes []*attributeSpec
	Globals    []*indexSpec
	Locals     []*indexSpec
}

func (spec *tableSpec) lsi(indexName string) *indexSpec {
	for _, lsi := range spec.Locals {
		if lsi.IndexName == indexName {
			return lsi
		}
	}

	lsi := &indexSpec{
		IndexName: indexName,
	}
	spec.Locals = append(spec.Locals, lsi)

	return lsi
}

func (spec *tableSpec) gsi(indexName string) *indexSpec {
	for _, gsi := range spec.Globals {
		if gsi.IndexName == indexName {
			return gsi
		}
	}

	gsi := &indexSpec{
		IndexName: indexName,
	}
	spec.Globals = append(spec.Globals, gsi)

	return gsi
}

func inspect(tableName string, model interface{}) (*tableSpec, error) {
	t, v := reflect.TypeOf(model), reflect.ValueOf(model)
	if t.Kind() == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("models must be structs.  %v is not a struct", t.String())
	}

	spec := tableSpec{
		TableName: tableName,
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		attrName, ok := getAttrName(field)
		if !ok {
			continue
		}

		attrType, err := getAttrType(field, v.Field(i))
		if err != nil {
			return nil, err
		}

		attr := &attributeSpec{
			FieldName:     field.Name,
			AttributeName: attrName,
			AttributeType: attrType,
		}

		spec.Attributes = append(spec.Attributes, attr)

		tags, ok := field.Tag.Lookup(tagKey)
		if !ok {
			continue
		}

		for _, tag := range strings.Split(tags, tagSeparator) {
			tag = strings.TrimSpace(tag)
			switch {
			case tag == tagHashKey:
				spec.HashKey = &keySpec{
					AttributeName: attr.AttributeName,
					AttributeType: attr.AttributeType,
				}

			case tag == tagRangeKey:
				spec.RangeKey = &keySpec{
					AttributeName: attr.AttributeName,
					AttributeType: attr.AttributeType,
				}

			case strings.HasPrefix(tag, tagGsiHash):
				// gsi_hash:
				indexName := firstOption(tag[len(tagGsiHash):])

				gsi := spec.gsi(indexName)
				gsi.IndexName = indexName
				gsi.KeysOnly = hasTagOption(tag, optionKeysOnly)
				gsi.HashKey = &keySpec{
					AttributeName: attr.AttributeName,
					AttributeType: attr.AttributeType,
				}

			case strings.HasPrefix(tag, tagGsiRange):
				// gsi_range:
				indexName := firstOption(tag[len(tagGsiRange):])

				gsi := spec.gsi(indexName)
				gsi.IndexName = indexName
				gsi.KeysOnly = hasTagOption(tag, optionKeysOnly)
				gsi.RangeKey = &keySpec{
					AttributeName: attr.AttributeName,
					AttributeType: attr.AttributeType,
				}

			case strings.HasPrefix(tag, tagGsi):
				// gsi:
				indexName := firstOption(tag[len(tagGsi):])

				gsi := spec.gsi(indexName)
				gsi.Attributes = append(gsi.Attributes, attr)

			case strings.HasPrefix(tag, tagLsiRange):
				// lsi_range:
				indexName := firstOption(tag[len(tagLsiRange):])

				lsi := spec.lsi(indexName)
				lsi.IndexName = indexName
				lsi.KeysOnly = hasTagOption(tag, optionKeysOnly)
				lsi.RangeKey = &keySpec{
					AttributeName: attr.AttributeName,
					AttributeType: attr.AttributeType,
				}

			case strings.HasPrefix(tag, tagLsi):
				// lsi:
				indexName := firstOption(tag[len(tagLsi):])

				lsi := spec.lsi(indexName)
				lsi.Attributes = append(lsi.Attributes, attr)
			}
		}
	}

	return &spec, nil
}

func getAttrType(field reflect.StructField, value reflect.Value) (string, error) {
	switch kind := field.Type.Kind(); kind {
	case reflect.String:
		return dynamodb.ScalarAttributeTypeS, nil
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		return dynamodb.ScalarAttributeTypeN, nil
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return dynamodb.ScalarAttributeTypeN, nil
	case reflect.Bool:
		return dynamodb.ScalarAttributeTypeB, nil
	default:
		// ok
	}

	if field.IsExported() {
		if v, ok := value.Interface().(dynamodbattribute.Marshaler); ok {
			item, err := dynamodbattribute.Marshal(v)
			if err != nil {
				return "", err
			}
			switch {
			case item.N != nil:
				return dynamodb.ScalarAttributeTypeN, nil
			case item.S != nil:
				return dynamodb.ScalarAttributeTypeS, nil
			case item.B != nil:
				return dynamodb.ScalarAttributeTypeB, nil
			}
		}
	}

	return "Unknown", nil
}

func firstOption(tag string) string {
	segments := strings.Split(tag, ",")
	return strings.TrimSpace(segments[0])
}

func hasTagOption(tag, option string) bool {
	for _, item := range strings.Split(tag, ",") {
		if strings.TrimSpace(item) == option {
			return true
		}
	}
	return false
}

func getAttrName(field reflect.StructField) (string, bool) {
	if v, ok := field.Tag.Lookup("dynamodbav"); ok {
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, "-") {
			return "", false
		}
		segments := strings.Split(v, ",")
		return strings.TrimSpace(segments[0]), true
	}

	return field.Name, true
}
