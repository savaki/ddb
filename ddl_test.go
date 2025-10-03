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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Sample struct {
	Hash  string `ddb:"hash"`
	Range int64  `ddb:"range"`
}

type LSI struct {
	Hash     string `ddb:"hash"`
	Range    int64  `ddb:"range"`
	AltRange int64  `ddb:"lsi_range:index" dynamodbav:"alt"`
	Hello    string `ddb:"lsi:index"       dynamodbav:"hello"`
}

type LSIKeysOnly struct {
	Hash     string `ddb:"hash"`
	Range    int64  `ddb:"range"`
	AltRange int64  `ddb:"lsi_range:index,keys_only" dynamodbav:"alt"`
	Hello    string
}

type GSI struct {
	Hash     string `ddb:"hash"`
	Range    int64  `ddb:"range"`
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
			WithBillingMode(string(types.BillingModePayPerRequest)),
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
			WithStreamSpecification(string(types.StreamViewTypeKeysOnly)),
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
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	// If UPDATE_TESTDATA env var is set, write the new data
	if os.Getenv("UPDATE_TESTDATA") == "1" {
		if err := ioutil.WriteFile(filename, data, 0644); err != nil {
			t.Fatalf("failed to write testdata: %v", err)
		}
		return
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
		t.Fatalf("#-- got %v\n#-- want %v %v", prettyJSON(got), filename, prettyJSON(want))
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
	ID   string `ddb:"hash"`
	Name string
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
			err: &types.ResourceInUseException{Message: aws.String("boom")},
		}
		table := New(mock).MustTable(tableName, Example{})
		err := table.CreateTableIfNotExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}

func TestDeleteTable(t *testing.T) {
	var (
		ctx       = context.Background()
		tableName = "blah"
	)

	t.Run("ok", func(t *testing.T) {
		mock := &Mock{}
		table := New(mock).MustTable(tableName, Example{})
		err := table.DeleteTableIfExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("table already exists", func(t *testing.T) {
		mock := &Mock{
			err: &types.ResourceNotFoundException{Message: aws.String("boom")},
		}
		table := New(mock).MustTable(tableName, Example{})
		err := table.DeleteTableIfExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("other error", func(t *testing.T) {
		mock := &Mock{
			err: &types.ConditionalCheckFailedException{Message: aws.String("boom")},
		}
		table := New(mock).MustTable(tableName, Example{})
		err := table.DeleteTableIfExists(ctx)
		if err == nil {
			t.Fatalf("got %v; want not nil", err)
		}
	})
}

func TestTable_CreateTableIfNotExists_Live(t *testing.T) {
	if !runIntegrationTests {
		t.SkipNow()
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("blah", "blah", "")),
	)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	api := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()

	t.Run("gsi - pay per request", func(t *testing.T) {
		type GSI struct {
			ID  string `ddb:"hash"`
			GID string `ddb:"gsi_hash:global"`
		}

		tableName := fmt.Sprintf("gsi-payper-%v", time.Now().UnixNano())
		table := New(api).MustTable(tableName, GSI{})

		err := table.CreateTableIfNotExists(ctx, WithBillingMode(string(types.BillingModePayPerRequest)))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		defer table.DeleteTableIfExists(ctx)
	})

	t.Run("gsi - hash only", func(t *testing.T) {
		type GSI struct {
			ID  string `ddb:"hash"`
			GID string `ddb:"gsi_hash:global"`
		}
		tableName := fmt.Sprintf("gsi-h-%v", time.Now().UnixNano())
		table := New(api).MustTable(tableName, GSI{})

		err := table.CreateTableIfNotExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		defer table.DeleteTableIfExists(ctx)

		want := GSI{
			ID:  "id",
			GID: "gid",
		}
		err = table.Put(want).Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		query := table.Query("#GID = ?", want.GID).
			IndexName("global")

		var got GSI
		err = query.First(&got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("gsi - hash and range", func(t *testing.T) {
		type GSI struct {
			ID    string `ddb:"hash"`
			Hash  string `ddb:"gsi_hash:global"`
			Range string `ddb:"gsi_range:global"`
		}
		tableName := fmt.Sprintf("gsi-hr-%v", time.Now().UnixNano())
		table := New(api).MustTable(tableName, GSI{})

		err := table.CreateTableIfNotExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		defer table.DeleteTableIfExists(ctx)

		want := GSI{
			ID:    "id",
			Hash:  "hash",
			Range: "range",
		}
		err = table.Put(want).Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		query := table.Query("#Hash = ? and #Range = ?", want.Hash, want.Range).
			IndexName("global")

		var got GSI
		err = query.First(&got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}
