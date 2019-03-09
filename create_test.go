// Copyright 2019 Matt Ho
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
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Sample struct {
	Hash  string `ddb:"hash_key"`
	Range int64  `ddb:"range_key"`
}

type LSI struct {
	Hash     string `ddb:"hash_key"`
	Range    int64  `ddb:"range_key"`
	AltRange int64  `ddb:"lsi_range:index" dynamodbav:"alt"`
	Hello    string `ddb:"lsi:index"       dynamodbav:"hello"`
}

type LSIKeysOnly struct {
	Hash     string `ddb:"hash_key"`
	Range    int64  `ddb:"range_key"`
	AltRange int64  `ddb:"lsi_range:index,keys_only" dynamodbav:"alt"`
	Hello    string
}

type GSI struct {
	Hash     string `ddb:"hash_key"`
	Range    int64  `ddb:"range_key"`
	AltHash  int64  `ddb:"gsi_hash:index"  dynamodbav:"h"`
	AltRange int64  `ddb:"gsi_range:index" dynamodbav:"r"`
	Hello    string `ddb:"gsi:index"       dynamodbav:"hello"`
}

func Test_makeCreateTableInput(t *testing.T) {
	const tableName = "blah"

	spec, err := inspect(tableName, Sample{})
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")

	t.Run("minimal", func(t *testing.T) {
		got := makeCreateTableInput(tableName, spec)
		assertEqual(t, got, "testdata/minimal.json")
	})

	t.Run("pay per request", func(t *testing.T) {
		got := makeCreateTableInput(tableName, spec,
			WithBillingMode(dynamodb.BillingModePayPerRequest),
		)
		assertEqual(t, got, "testdata/pay_per_request.json")
	})

	t.Run("custom throughput", func(t *testing.T) {
		rcap := int64(4)
		wcap := int64(5)
		got := makeCreateTableInput(tableName, spec,
			WithReadCapacity(rcap),
			WithWriteCapacity(wcap),
		)
		assertEqual(t, got, "testdata/custom_throughput.json")
	})

	t.Run("stream specification", func(t *testing.T) {
		got := makeCreateTableInput(tableName, spec,
			WithStreamSpecification(dynamodb.StreamViewTypeKeysOnly),
		)
		assertEqual(t, got, "testdata/stream_specification.json")
	})

	t.Run("lsi", func(t *testing.T) {
		lsi, err := inspect("example", LSI{})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := makeCreateTableInput(tableName, lsi) //WithLocalSecondaryIndex(indexName, projectionType, WithAttr(attributeName, dynamodb.ScalarAttributeTypeS)),
		assertEqual(t, got, "testdata/lsi.json")
	})

	t.Run("lsi, keys only", func(t *testing.T) {
		lsi, err := inspect("example", LSIKeysOnly{})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := makeCreateTableInput(tableName, lsi) //WithLocalSecondaryIndex(indexName, projectionType, WithAttr(attributeName, dynamodb.ScalarAttributeTypeS)),
		assertEqual(t, got, "testdata/lsi_keys_only.json")
	})

	t.Run("gsi", func(t *testing.T) {
		gsi, err := inspect("example", GSI{})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := makeCreateTableInput(tableName, gsi) //WithGlobalSecondaryIndex(indexName, projectionType, WithAttr(attributeName, dynamodb.ScalarAttributeTypeS)),
		assertEqual(t, got, "testdata/gsi.json")
	})
}

func assertEqual(t *testing.T, v interface{}, filename string) {
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	data, err = ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var want map[string]interface{}
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("#-- got ----------------\n%v\n#-- want ----------------\n%v", prettyJSON(got), prettyJSON(want))
	}
}

func prettyJSON(v interface{}) string {
	buf := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(buf)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(v)
	return buf.String()
}

type Example struct {
}

func TestCreateTable(t *testing.T) {
	var (
		ctx       = context.Background()
		tableName = "blah"
	)

	t.Run("ok", func(t *testing.T) {
		mock := &Mock{}
		table := New(mock).MustTable(tableName, Example{})
		err := table.CreateTableIfNotExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("table already exists", func(t *testing.T) {
		mock := &Mock{
			err: awserr.New(dynamodb.ErrCodeResourceInUseException, "boom", nil),
		}
		table := New(mock).MustTable(tableName, Example{})
		err := table.CreateTableIfNotExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}
