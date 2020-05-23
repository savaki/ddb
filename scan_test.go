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

type ScanTable struct {
	ID   string `dynamodbav:"id" ddb:"hash"`
	Name string `dynamodb:"name" ddb:"gsi_hash:gsi"`
}

func TestScan_First(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			want  = ScanTable{ID: "abc"}
			mock  = &Mock{scanItems: []interface{}{want}}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got ScanTable
		err := table.Scan().First(&got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("not found", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got ScanTable
		err := table.Scan().First(&got)
		if !IsItemNotFoundError(err) {
			t.Fatalf("got %#v; want ErrItemNotFound", err)
		}
	})

	t.Run("aws err", func(t *testing.T) {
		var (
			want  = io.EOF
			mock  = &Mock{err: io.EOF}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got ScanTable
		err := table.Scan().First(&got)
		if err == nil {
			t.Fatalf("got %v; want %v", err, want)
		}
	})
}

func TestScan_Each(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			item1 = ScanTable{ID: "abc"}
			item2 = ScanTable{ID: "def"}
			want  = []ScanTable{item1, item2}
			mock  = &Mock{scanItems: []interface{}{item1, item2}}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got []ScanTable
		err := table.Scan().Each(func(item Item) (bool, error) {
			var v ScanTable
			if err := item.Unmarshal(&v); err != nil {
				return false, nil
			}
			got = append(got, v)
			return true, nil
		})

		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, item1)
		}
	})

	t.Run("stop early", func(t *testing.T) {
		var (
			item1 = ScanTable{ID: "abc"}
			item2 = ScanTable{ID: "def"}
			want  = []ScanTable{item1}
			mock  = &Mock{scanItems: []interface{}{item1, item2}}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got []ScanTable
		err := table.Scan().Each(func(item Item) (bool, error) {
			var v ScanTable
			if err := item.Unmarshal(&v); err != nil {
				return false, nil
			}
			got = append(got, v)
			return false, nil
		})

		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, item1)
		}
	})
}

func TestScan_Condition(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		input := table.Scan().
			Filter("#ID = ?", "abc").
			makeScanInput(0, 1, nil)

		assertEqual(t, input, "testdata/scan_condition.json")
	})
}

func TestScan_IndexName(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		input := table.Scan().
			IndexName("gsi").
			makeScanInput(0, 1, nil)

		assertEqual(t, input, "testdata/scan_index.json")
	})
}

func TestScan_ConditionLive(t *testing.T) {
	if !runIntegrationTests {
		t.SkipNow()
	}

	type Sample struct {
		ID string `ddb:"hash"`
	}

	var (
		ctx  = context.Background()
		s, _ = session.NewSession(aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
			WithRegion("us-west-2").
			WithEndpoint("http://localhost:8000"))
		api       = dynamodb.New(s)
		tableName = fmt.Sprintf("scan-%v", time.Now().UnixNano())
		table     = New(api).MustTable(tableName, Sample{})
	)

	err := table.CreateTableIfNotExists(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	defer table.DeleteTableIfExists(ctx)

	err = table.Put(Sample{ID: "a"}).RunWithContext(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	err = table.Put(Sample{ID: "b"}).RunWithContext(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	err = table.Put(Sample{ID: "c"}).RunWithContext(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var samples []Sample
	fn := func(item Item) (bool, error) {
		var sample Sample
		if err := item.Unmarshal(&sample); err != nil {
			return false, err
		}
		samples = append(samples, sample)
		return true, nil
	}

	err = table.Scan().
		ConsistentRead(true).
		Filter("#ID = ?", "b").
		TotalSegments(3).
		EachWithContext(ctx, fn)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if got, want := len(samples), 1; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
	want := Sample{ID: "b"}
	if got := samples[0]; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestScan_ConsistentRead(t *testing.T) {
	s := &Scan{
		expr: &expression{},
		spec: &tableSpec{TableName: "example"},
	}
	s.ConsistentRead(true)
	input := s.makeScanInput(1, 2, nil)

	if input.ConsistentRead == nil {
		t.Fatalf("got nil; want not nil")
	}
	if !*input.ConsistentRead {
		t.Fatalf("got false; want true")
	}
}

func TestScan_ConsumedCapacity(t *testing.T) {
	type Sample struct {
		ID string `ddb:"hash"`
	}

	var (
		mock = &Mock{
			readUnits:  1,
			writeUnits: 2,
		}
		table    = New(mock).MustTable("blah", Sample{})
		callback = func(item Item) (bool, error) { return true, nil }
		consumed ConsumedCapacity
	)

	err := table.Scan().ConsumedCapacity(&consumed).Each(callback)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if got, want := consumed.ReadUnits, mock.readUnits; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
	if got, want := consumed.WriteUnits, mock.writeUnits; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}
