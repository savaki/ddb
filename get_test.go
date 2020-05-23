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
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type GetExample struct {
	ID string `ddb:"hash"`
}

func TestGet_One(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			want = GetExample{ID: "abc"}
			mock = &Mock{
				getItem:    want,
				readUnits:  1,
				writeUnits: 2,
			}
			table    = New(mock).MustTable("example", GetExample{})
			capacity ConsumedCapacity
		)

		err := table.Put(want).Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		var got GetExample
		err = table.Get("abc").ConsumedCapacity(&capacity).Scan(&got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}

		if got, want := capacity.ReadUnits, mock.readUnits; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := capacity.WriteUnits, mock.writeUnits; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}

		consumed := table.ConsumedCapacity()
		if got, want := consumed.ReadUnits, mock.readUnits*2; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := consumed.WriteUnits, mock.writeUnits*2; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("aws api failed", func(t *testing.T) {
		var (
			want  = io.EOF
			mock  = &Mock{err: want}
			table = New(mock).MustTable("example", GetExample{})
		)

		var blah GetExample
		got := table.Get("abc").Scan(&blah)
		if got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("not found", func(t *testing.T) {
		var (
			mock  = &Mock{}
			table = New(mock).MustTable("example", GetExample{})
		)

		var blah GetExample
		err := table.Get("abc").Scan(&blah)
		if !IsItemNotFoundError(err) {
			t.Fatalf("got %v; want ErrItemNotFound", err)
		}
	})
}

func TestLive(t *testing.T) {
	if !runIntegrationTests {
		t.SkipNow()
	}

	var (
		ctx  = context.Background()
		s, _ = session.NewSession(aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
			WithRegion("us-west-2").
			WithEndpoint("http://localhost:8000"))
		api       = dynamodb.New(s)
		tableName = fmt.Sprintf("tmp-%v", time.Now().UnixNano())
		table     = New(api).MustTable(tableName, GetExample{})
		want      = GetExample{ID: "abc"}
	)

	err := table.CreateTableIfNotExists(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	err = table.Put(want).Run()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var got GetExample
	err = table.Get(want.ID).ScanWithContext(ctx, &got)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestGet_Range(t *testing.T) {
	want := "abc"
	g := &Get{}
	g.Range(want)

	if got := g.rangeKey; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestGet_ConsistentRead(t *testing.T) {
	g := &Get{
		spec: &tableSpec{TableName: "example"},
	}
	g.ConsistentRead(true)
	input, err := g.GetItemInput()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if input.ConsistentRead == nil {
		t.Fatalf("got nil; expected not nil")
	}
	if !*input.ConsistentRead {
		t.Fatalf("got false; expected true")
	}
}
