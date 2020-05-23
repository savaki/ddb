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
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Simple struct {
	Hash  string `ddb:"hash"`
	Range string `ddb:"range"`
}

func Test_makeKey(t *testing.T) {
	spec, err := inspect("simple", Simple{})
	if err != nil {
		t.Fatalf("got %#v; want nil", err)
	}

	item, err := makeKey(spec, "abc", "def")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	assertEqual(t, item, "testdata/keys.json")
}

func Test_marshal(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		want := map[string]*dynamodb.AttributeValue{
			"hello": {S: aws.String("world")},
		}
		got, err := marshal(want)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got.M == nil {
			t.Fatalf("got nil; want not nil")
		}
		if !reflect.DeepEqual(got.M, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("list", func(t *testing.T) {
		want := []*dynamodb.AttributeValue{
			{S: aws.String("hello")},
			{S: aws.String("world")},
		}
		got, err := marshal(want)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got.L == nil {
			t.Fatalf("got nil; want not nil")
		}
		if !reflect.DeepEqual(got.L, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func Test_marshalMap(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		want := map[string]*dynamodb.AttributeValue{
			"hello": {S: aws.String("world")},
		}
		got, err := marshalMap(want)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("interface{}", func(t *testing.T) {
		want := "world"
		raw := map[string]string{
			"hello": want,
		}
		item, err := marshalMap(raw)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if item == nil {
			t.Fatalf("got nil; want not nil")
		}
		if item["hello"] == nil {
			t.Fatalf("got nil; want not nil")
		}
		if item["hello"].S == nil {
			t.Fatalf("got nil; want not nil")
		}
		if got := *item["hello"].S; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestDynamodbMarshal(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		want := "abc"
		item, err := marshal(want)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if item.S == nil {
			t.Fatalf("got nil; want not nil")
		}
		if item.S == nil {
			t.Fatalf("got nil; want not nil")
		}
		if got := *item.S; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("int64", func(t *testing.T) {
		want := int64(123)
		item, err := marshal(want)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if item.N == nil {
			t.Fatalf("got nil; want not nil")
		}
		if item.N == nil {
			t.Fatalf("got nil; want not nil")
		}
		if got := *item.N; got != strconv.FormatInt(want, 10) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("nil", func(t *testing.T) {
		item, err := marshal(nil)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if item == nil {
			t.Fatalf("got nil; want not nil")
		}
	})

	t.Run("*dynamodb.AttributeValue", func(t *testing.T) {
		want := &dynamodb.AttributeValue{S: aws.String("abc")}
		got, err := marshal(want)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got == nil {
			t.Fatalf("got nil; want not nil")
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}
