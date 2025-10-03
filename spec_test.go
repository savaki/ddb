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
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Model struct {
	Hash     string `ddb:"hash"`
	Range    int64  `ddb:"range"`
	AltRange uint64 `ddb:"lsi_range:local" dynamodbav:"alt"`
}

func TestInspect(t *testing.T) {
	spec, err := inspect("example", Model{})
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	t.Run("hash", func(t *testing.T) {
		if spec.HashKey == nil {
			t.Fatalf("got nil; want not nill")
		}
		if got, want := spec.HashKey.AttributeName, "Hash"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := spec.HashKey.AttributeType, "S"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("range", func(t *testing.T) {
		if spec.RangeKey == nil {
			t.Fatalf("got nil; want not nill")
		}
		if got, want := spec.RangeKey.AttributeName, "Range"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := spec.RangeKey.AttributeType, "N"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("lsi using dynamodb", func(t *testing.T) {
		v := spec.lsi("local")
		if v.RangeKey == nil {
			t.Fatalf("got nil; want not nil")
		}
		if got, want := v.RangeKey.AttributeName, "alt"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := v.RangeKey.AttributeType, "N"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("attributes", func(t *testing.T) {
		want := []*attributeSpec{
			{
				FieldName:     "Hash",
				AttributeName: "Hash",
				AttributeType: "S",
			},
			{
				FieldName:     "Range",
				AttributeName: "Range",
				AttributeType: "N",
			},
			{
				FieldName:     "AltRange",
				AttributeName: "alt",
				AttributeType: "N",
			},
		}

		if got := spec.Attributes; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}
	})
}

type Key struct {
	S string
}

func (k Key) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	return &types.AttributeValueMemberS{Value: k.S}, nil
}

func (k *Key) UnmarshalDynamoDBAttributeValue(item types.AttributeValue) error {
	if s, ok := item.(*types.AttributeValueMemberS); ok {
		*k = Key{S: s.Value}
	}
	return nil
}

type Custom struct {
	Key Key `ddb:"hash" dynamodbav:"pk"`
}

func TestInspectCustomMarshal(t *testing.T) {
	spec, err := inspect("custom", Custom{})
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	want := &keySpec{
		AttributeName: "pk",
		AttributeType: "S",
	}
	if got := spec.HashKey; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v; want %#v", got, want)
	}
	if spec.RangeKey != nil {
		t.Fatalf("got %v; want nil", spec.RangeKey)
	}
}
