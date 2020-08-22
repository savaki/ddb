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
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func withTable(t *testing.T, schema interface{}, callback func(ctx context.Context, table *Table)) {
	var (
		s = session.Must(session.NewSession(aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
			WithEndpoint("http://localhost:8000").
			WithRegion("us-west-2")))
		api       = dynamodb.New(s)
		client    = New(api)
		tableName = fmt.Sprintf("table-%v", time.Now().UnixNano())
		table     = client.MustTable(tableName, schema)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// appointment
	err := table.CreateTableIfNotExists(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	defer table.DeleteTableIfExists(ctx)

	callback(ctx, table)
}

type QueryExample struct {
	ID   string `ddb:"hash"`
	Date string `ddb:"range"`
}

func TestQuery(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			want  = QueryExample{ID: "abc", Date: "2019-03-10"}
			mock  = &Mock{queryItems: []interface{}{want}}
			table = New(mock).MustTable("example", QueryExample{})
		)

		query := table.Query("#ID = ?", want.ID)
		query.IndexName("index")
		query.KeyCondition("#Date = ?", want.Date)
		query.ConsistentRead(true)
		query.ScanIndexForward(true)

		var got QueryExample
		fn := func(item Item) (bool, error) {
			if err := item.Unmarshal(&got); err != nil {
				return false, err
			}
			return true, nil
		}
		err := query.Each(fn)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}

		assertEqual(t, mock.queryInput, "testdata/query_ok.json")
	})

	t.Run("fn returns err", func(t *testing.T) {
		var (
			item  = QueryExample{ID: "abc", Date: "2019-03-10"}
			mock  = &Mock{queryItems: []interface{}{item}}
			table = New(mock).MustTable("example", QueryExample{})
		)

		query := table.Query(item.ID)
		want := fmt.Errorf("boom")
		fn := func(item Item) (bool, error) {
			return false, want
		}
		err := query.Each(fn)
		if err != want {
			t.Fatalf("got %v; want %v", err, item)
		}
	})

	t.Run("fn returns false", func(t *testing.T) {
		var (
			item  = QueryExample{ID: "abc", Date: "2019-03-10"}
			mock  = &Mock{queryItems: []interface{}{item, item, item}}
			table = New(mock).MustTable("example", QueryExample{})
		)

		query := table.Query(item.ID)
		count := 0
		fn := func(item Item) (bool, error) {
			count++
			return false, nil
		}
		err := query.Each(fn)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := count, 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestQuery_First(t *testing.T) {
	t.Run("first returns first item", func(t *testing.T) {
		var (
			want  = QueryExample{ID: "abc", Date: "2019-03-10"}
			blah  = QueryExample{ID: "blah", Date: "2019-03-11"}
			mock  = &Mock{queryItems: []interface{}{want, blah, blah}}
			table = New(mock).MustTable("example", QueryExample{})
		)

		var got QueryExample
		err := table.Query(want.ID).First(&got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("fails", func(t *testing.T) {
		var (
			want  = QueryExample{ID: "abc", Date: "2019-03-10"}
			blah  = QueryExample{ID: "blah", Date: "2019-03-11"}
			mock  = &Mock{queryItems: []interface{}{want, blah, blah}}
			table = New(mock).MustTable("example", QueryExample{})
		)

		err := table.Query(want.ID).First(nil)
		if err == nil {
			t.Fatalf("got nil; want not nil")
		}
	})

	t.Run("not found", func(t *testing.T) {
		var (
			mock  = &Mock{}
			table = New(mock).MustTable("example", QueryExample{})
		)

		var got QueryExample
		err := table.Query("not-found").First(got)
		if err == nil {
			t.Fatalf("got nil; want not nil")
		}
		if v, ok := err.(interface{ Code() string }); !ok || v.Code() != ErrItemNotFound {
			t.Fatalf("got %v; want ErrItemNotFound", err)
		}
	})
}

func TestQuery_Filter(t *testing.T) {
	type Sample struct {
		Hash  string `ddb:"hash"`
		Range int    `ddb:"range"`
		Value int
	}

	var (
		mock  = &Mock{}
		table = New(mock).MustTable("example", Sample{})
	)

	t.Run("ok", func(t *testing.T) {
		query := table.Query("#Hash = ?", "abc").
			Filter("#Value between ? and ?", 1, 3)

		input, err := query.QueryInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		assertEqual(t, input, "testdata/query_filter.json")
	})

	t.Run("fails", func(t *testing.T) {
		query := table.Query("#Hash = ?", "abc").
			Filter("#Value between ?")

		_, err := query.QueryInput()
		if err == nil {
			t.Fatalf("got nil; want not nil")
		}
	})
}

func TestQuery_EachWithContext(t *testing.T) {
	const pk = "pk"

	type Record struct {
		PK string `dynamodb:"pk" ddb:"hash"`
		SK int    `dynamodb:"sk" ddb:"range"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		const n = 10
		for i := 0; i < n; i++ {
			record := Record{
				PK: pk,
				SK: i,
			}

			err := table.Put(record).Run()
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}

		findAll := func(query *Query) (int, map[string]*dynamodb.AttributeValue, string, error) {
			var records []Record
			callback := func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, nil
				}
				records = append(records, r)
				return true, nil
			}

			var lastEvaluatedKey map[string]*dynamodb.AttributeValue
			var lastToken string
			query = query.
				LastEvaluatedKey(&lastEvaluatedKey).
				LastEvaluatedToken(&lastToken)

			if err := query.Each(callback); err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			return len(records), lastEvaluatedKey, lastToken, nil
		}

		t.Run("all", func(t *testing.T) {
			query := table.Query("#PK = ?", pk)

			got, lastKey, lastToken, err := findAll(query)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}
			if want := n; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
			if got, want := len(lastKey), 0; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
			if got, want := lastToken, ""; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
		})

		t.Run("paginate by key", func(t *testing.T) {
			for _, want := range []int{1, 2, 4, 8} {
				t.Run(fmt.Sprintf("limit %v", want), func(t *testing.T) {
					query := table.Query("#PK = ?", pk).
						Limit(int64(want))

					got, lastKey, _, err := findAll(query)
					if err != nil {
						t.Fatalf("got %v; want nil", err)
					}
					if got != want {
						t.Fatalf("got %v; want %v", got, want)
					}
					if lastKey == nil {
						t.Fatalf("got nil; want not nil")
					}

					// remainder
					query = table.Query("#PK = ?", pk).
						StartKey(lastKey)

					remain, lastKey, _, err := findAll(query)
					if err != nil {
						t.Fatalf("got %v; want nil", err)
					}
					if got, want := got+remain, n; got != want {
						t.Fatalf("got %v; want %v", got, want)
					}
				})
			}
		})

		t.Run("paginate by token", func(t *testing.T) {
			for _, want := range []int{1, 2, 4, 8} {
				t.Run(fmt.Sprintf("limit %v", want), func(t *testing.T) {
					query := table.Query("#PK = ?", pk).
						Limit(int64(want))

					got, _, lastToken, err := findAll(query)
					if err != nil {
						t.Fatalf("got %v; want nil", err)
					}
					if got != want {
						t.Fatalf("got %v; want %v", got, want)
					}
					if lastToken == "" {
						t.Fatalf("got blank; want not not blank")
					}

					// remainder
					query = table.Query("#PK = ?", pk).
						StartToken(lastToken)

					remain, _, lastToken, err := findAll(query)
					if err != nil {
						t.Fatalf("got %v; want nil", err)
					}
					if got, want := got+remain, n; got != want {
						t.Fatalf("got %v; want %v", got, want)
					}
				})
			}
		})
	})
}

func TestQuery_FindAllWithContext(t *testing.T) {
	type Record struct {
		PK string `dynamodb:"pk" ddb:"hash"`
		SK int    `dynamodb:"sk" ddb:"range"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		record := Record{
			PK: "pk",
			SK: 123,
		}
		err := table.Put(record).Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		t.Run("struct", func(t *testing.T) {
			var records []Record
			query := table.Query("#PK = ?", "pk")
			err = query.FindAll(&records)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(records), 1; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
			if got, want := records[0].PK, record.PK; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
			if got, want := records[0].SK, record.SK; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
		})

		t.Run("struct", func(t *testing.T) {
			var records []*Record
			query := table.Query("#PK = ?", "pk")
			err = query.FindAll(&records)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(records), 1; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
			if got, want := records[0].PK, record.PK; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
			if got, want := records[0].SK, record.SK; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
		})
	})
}
