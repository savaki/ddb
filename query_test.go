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
	"encoding/base64"
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

func TestQuery_LastEvaluatedKey(t *testing.T) {
	const pk = "pk"

	type Record struct {
		PK string `dynamodbav:"pk" ddb:"hash"`
		SK int    `dynamodbav:"sk" ddb:"range"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		// Insert 10 records with SK 0-9
		const n = 10
		for i := 0; i < n; i++ {
			record := Record{PK: pk, SK: i}
			if err := table.Put(record).Run(); err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}

		t.Run("limit returns key of last seen item", func(t *testing.T) {
			// With Limit(3), we should see SK 0, 1, 2
			// LastEvaluatedKey should point to the item with SK=2
			var records []Record
			var lastKey map[string]*dynamodb.AttributeValue

			query := table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				Limit(3).
				LastEvaluatedKey(&lastKey)

			err := query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				records = append(records, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(records), 3; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}
			if lastKey == nil {
				t.Fatalf("lastKey is nil; want not nil")
			}

			// Verify the key matches the last record seen (SK=2)
			lastRecord := records[len(records)-1]
			if got := *lastKey["pk"].S; got != lastRecord.PK {
				t.Fatalf("lastKey pk = %v; want %v", got, lastRecord.PK)
			}
			if got := *lastKey["sk"].N; got != fmt.Sprintf("%d", lastRecord.SK) {
				t.Fatalf("lastKey sk = %v; want %v", got, lastRecord.SK)
			}

			// Use the key to continue and verify we get the next items
			var moreRecords []Record
			query = table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				StartKey(lastKey).
				Limit(3)

			err = query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				moreRecords = append(moreRecords, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			// Should get SK 3, 4, 5 (items after SK=2)
			if got, want := len(moreRecords), 3; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}
			if got, want := moreRecords[0].SK, 3; got != want {
				t.Fatalf("first continued record SK = %v; want %v", got, want)
			}
		})

		t.Run("early termination returns key of last seen item", func(t *testing.T) {
			// Stop after seeing 2 items (SK 0, 1)
			// LastEvaluatedKey should point to the item with SK=1
			var records []Record
			var lastKey map[string]*dynamodb.AttributeValue

			query := table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				LastEvaluatedKey(&lastKey)

			err := query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				records = append(records, r)
				// Stop after 2 items
				return len(records) < 2, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(records), 2; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}
			if lastKey == nil {
				t.Fatalf("lastKey is nil; want not nil")
			}

			// Verify the key matches the last record seen (SK=1)
			lastRecord := records[len(records)-1]
			if got := *lastKey["pk"].S; got != lastRecord.PK {
				t.Fatalf("lastKey pk = %v; want %v", got, lastRecord.PK)
			}
			if got := *lastKey["sk"].N; got != fmt.Sprintf("%d", lastRecord.SK) {
				t.Fatalf("lastKey sk = %v; want %v", got, lastRecord.SK)
			}

			// Use the key to continue and verify we get the next items
			var moreRecords []Record
			query = table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				StartKey(lastKey)

			err = query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				moreRecords = append(moreRecords, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			// Should get SK 2-9 (8 items after SK=1)
			if got, want := len(moreRecords), 8; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}
			if got, want := moreRecords[0].SK, 2; got != want {
				t.Fatalf("first continued record SK = %v; want %v", got, want)
			}
		})

		t.Run("complete iteration returns nil key", func(t *testing.T) {
			var lastKey map[string]*dynamodb.AttributeValue
			var count int

			query := table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				LastEvaluatedKey(&lastKey)

			err := query.Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := count, n; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}
			if lastKey != nil && len(lastKey) > 0 {
				t.Fatalf("lastKey = %v; want nil or empty", lastKey)
			}
		})

		t.Run("token matches key for pagination", func(t *testing.T) {
			// Verify token and key produce the same pagination behavior
			var lastKey map[string]*dynamodb.AttributeValue
			var lastToken string

			query := table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				Limit(5).
				LastEvaluatedKey(&lastKey).
				LastEvaluatedToken(&lastToken)

			var firstBatch []Record
			err := query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				firstBatch = append(firstBatch, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if lastToken == "" {
				t.Fatalf("lastToken is empty; want not empty")
			}

			// Continue with key
			var keyBatch []Record
			query = table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				StartKey(lastKey)
			err = query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				keyBatch = append(keyBatch, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			// Continue with token
			var tokenBatch []Record
			query = table.Query("#PK = ?", pk).
				ScanIndexForward(true). // ascending order
				StartToken(lastToken)
			err = query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				tokenBatch = append(tokenBatch, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			// Both should produce the same results
			if got, want := len(keyBatch), len(tokenBatch); got != want {
				t.Fatalf("keyBatch len = %v; tokenBatch len = %v", got, want)
			}
			for i := range keyBatch {
				if keyBatch[i].SK != tokenBatch[i].SK {
					t.Fatalf("keyBatch[%d].SK = %v; tokenBatch[%d].SK = %v", i, keyBatch[i].SK, i, tokenBatch[i].SK)
				}
			}
		})
	})
}

func TestQuery_LastEvaluatedKey_GSI(t *testing.T) {
	const pk = "pk"

	type Record struct {
		PK    string `dynamodbav:"pk" ddb:"hash"`
		SK    int    `dynamodbav:"sk" ddb:"range"`
		GSIPK string `dynamodbav:"gsi_pk" ddb:"gsi_hash:gsi1"`
		GSISK int    `dynamodbav:"gsi_sk" ddb:"gsi_range:gsi1"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		// Insert 10 records with SK 0-9, GSI keys inverted
		const n = 10
		for i := 0; i < n; i++ {
			record := Record{
				PK:    pk,
				SK:    i,
				GSIPK: "gsi-" + pk,
				GSISK: n - 1 - i, // Inverted order
			}
			if err := table.Put(record).Run(); err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}

		t.Run("GSI pagination returns correct key structure", func(t *testing.T) {
			var records []Record
			var lastKey map[string]*dynamodb.AttributeValue

			query := table.Query("#GSIPK = ?", "gsi-"+pk).
				IndexName("gsi1").
				ScanIndexForward(true).
				Limit(3).
				LastEvaluatedKey(&lastKey)

			err := query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				records = append(records, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(records), 3; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}
			if lastKey == nil {
				t.Fatalf("lastKey is nil; want not nil")
			}

			// GSI key should include: table hash, table range, GSI hash, GSI range
			if _, ok := lastKey["pk"]; !ok {
				t.Fatal("lastKey missing table hash key 'pk'")
			}
			if _, ok := lastKey["sk"]; !ok {
				t.Fatal("lastKey missing table range key 'sk'")
			}
			if _, ok := lastKey["gsi_pk"]; !ok {
				t.Fatal("lastKey missing GSI hash key 'gsi_pk'")
			}
			if _, ok := lastKey["gsi_sk"]; !ok {
				t.Fatal("lastKey missing GSI range key 'gsi_sk'")
			}

			// Continue pagination and verify we get the next items
			var moreRecords []Record
			query = table.Query("#GSIPK = ?", "gsi-"+pk).
				IndexName("gsi1").
				ScanIndexForward(true).
				StartKey(lastKey).
				Limit(3)

			err = query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				moreRecords = append(moreRecords, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(moreRecords), 3; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}

			// Verify continuation: first continued record should have GSISK = 3
			// (since we got 0,1,2 in first batch)
			if got, want := moreRecords[0].GSISK, 3; got != want {
				t.Fatalf("first continued record GSISK = %v; want %v", got, want)
			}
		})
	})
}

func TestQuery_LastEvaluatedKey_LSI(t *testing.T) {
	const pk = "pk"

	type Record struct {
		PK    string `dynamodbav:"pk" ddb:"hash"`
		SK    int    `dynamodbav:"sk" ddb:"range"`
		LSISK int    `dynamodbav:"lsi_sk" ddb:"lsi_range:lsi1"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		// Insert 10 records with SK 0-9, LSI keys inverted
		const n = 10
		for i := 0; i < n; i++ {
			record := Record{
				PK:    pk,
				SK:    i,
				LSISK: n - 1 - i, // Inverted order
			}
			if err := table.Put(record).Run(); err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}

		t.Run("LSI pagination returns correct key structure", func(t *testing.T) {
			var records []Record
			var lastKey map[string]*dynamodb.AttributeValue

			query := table.Query("#PK = ?", pk).
				IndexName("lsi1").
				ScanIndexForward(true).
				Limit(3).
				LastEvaluatedKey(&lastKey)

			err := query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				records = append(records, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(records), 3; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}
			if lastKey == nil {
				t.Fatalf("lastKey is nil; want not nil")
			}

			// LSI key should include: table hash, table range, LSI range
			if _, ok := lastKey["pk"]; !ok {
				t.Fatal("lastKey missing table hash key 'pk'")
			}
			if _, ok := lastKey["sk"]; !ok {
				t.Fatal("lastKey missing table range key 'sk'")
			}
			if _, ok := lastKey["lsi_sk"]; !ok {
				t.Fatal("lastKey missing LSI range key 'lsi_sk'")
			}

			// Continue pagination and verify we get the next items
			var moreRecords []Record
			query = table.Query("#PK = ?", pk).
				IndexName("lsi1").
				ScanIndexForward(true).
				StartKey(lastKey).
				Limit(3)

			err = query.Each(func(item Item) (bool, error) {
				var r Record
				if err := item.Unmarshal(&r); err != nil {
					return false, err
				}
				moreRecords = append(moreRecords, r)
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := len(moreRecords), 3; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}

			// Verify continuation: first continued record should have LSISK = 3
			if got, want := moreRecords[0].LSISK, 3; got != want {
				t.Fatalf("first continued record LSISK = %v; want %v", got, want)
			}
		})
	})
}

func TestQuery_LastEvaluatedKey_HashOnly(t *testing.T) {
	type Record struct {
		PK   string `dynamodbav:"pk" ddb:"hash"`
		Data string `dynamodbav:"data"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		// Insert multiple records with different PKs
		const n = 5
		for i := 0; i < n; i++ {
			record := Record{
				PK:   fmt.Sprintf("pk-%d", i),
				Data: fmt.Sprintf("data-%d", i),
			}
			if err := table.Put(record).Run(); err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}

		t.Run("hash-only table pagination", func(t *testing.T) {
			// For hash-only tables, we need to use Scan instead of Query
			// But we can still test the extractKey logic with a single-item query
			var lastKey map[string]*dynamodb.AttributeValue

			query := table.Query("#PK = ?", "pk-0").
				LastEvaluatedKey(&lastKey)

			var count int
			err := query.Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if got, want := count, 1; got != want {
				t.Fatalf("got %v records; want %v", got, want)
			}

			// After complete iteration, lastKey should be nil
			if lastKey != nil && len(lastKey) > 0 {
				t.Fatalf("lastKey = %v; want nil or empty", lastKey)
			}
		})

		t.Run("hash-only early termination", func(t *testing.T) {
			var lastKey map[string]*dynamodb.AttributeValue

			query := table.Query("#PK = ?", "pk-0").
				LastEvaluatedKey(&lastKey)

			err := query.Each(func(item Item) (bool, error) {
				return false, nil // Stop immediately
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			// lastKey should contain only the hash key
			if lastKey == nil {
				t.Fatal("lastKey is nil; want not nil")
			}
			if _, ok := lastKey["pk"]; !ok {
				t.Fatal("lastKey missing hash key 'pk'")
			}
			// Should not have any range key
			if len(lastKey) != 1 {
				t.Fatalf("lastKey has %d keys; want 1 (hash only)", len(lastKey))
			}
		})
	})
}

func TestQuery_StartToken_Errors(t *testing.T) {
	type Record struct {
		PK string `dynamodbav:"pk" ddb:"hash"`
		SK int    `dynamodbav:"sk" ddb:"range"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		t.Run("invalid base64 token", func(t *testing.T) {
			query := table.Query("#PK = ?", "pk").StartToken("!!invalid-base64!!")
			err := query.Each(func(item Item) (bool, error) {
				return true, nil
			})
			if err == nil {
				t.Fatal("got nil; want error")
			}
		})

		t.Run("invalid JSON in token", func(t *testing.T) {
			// Valid base64 but invalid JSON
			token := base64.StdEncoding.EncodeToString([]byte("not valid json"))
			query := table.Query("#PK = ?", "pk").StartToken(token)
			err := query.Each(func(item Item) (bool, error) {
				return true, nil
			})
			if err == nil {
				t.Fatal("got nil; want error")
			}
		})

		t.Run("empty token treated as no start key", func(t *testing.T) {
			// Insert a record
			if err := table.Put(Record{PK: "pk", SK: 1}).Run(); err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			query := table.Query("#PK = ?", "pk").StartToken("")
			var count int
			err := query.Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}
			if count == 0 {
				t.Fatal("got 0 records; want at least 1")
			}
		})
	})
}

func TestQuery_CallbackError(t *testing.T) {
	const pk = "pk"

	type Record struct {
		PK string `dynamodbav:"pk" ddb:"hash"`
		SK int    `dynamodbav:"sk" ddb:"range"`
	}

	withTable(t, Record{}, func(ctx context.Context, table *Table) {
		// Insert 5 records
		const n = 5
		for i := 0; i < n; i++ {
			record := Record{PK: pk, SK: i}
			if err := table.Put(record).Run(); err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}

		t.Run("callback error propagates correctly", func(t *testing.T) {
			var lastKey map[string]*dynamodb.AttributeValue
			expectedErr := fmt.Errorf("callback error")

			var processedCount int
			query := table.Query("#PK = ?", pk).
				ScanIndexForward(true).
				LastEvaluatedKey(&lastKey)

			err := query.Each(func(item Item) (bool, error) {
				processedCount++
				if processedCount == 3 {
					return false, expectedErr
				}
				return true, nil
			})

			if err != expectedErr {
				t.Fatalf("got %v; want %v", err, expectedErr)
			}
			if processedCount != 3 {
				t.Fatalf("processed %d items; want 3", processedCount)
			}
		})

		t.Run("unmarshal error in callback", func(t *testing.T) {
			var lastKey map[string]*dynamodb.AttributeValue

			query := table.Query("#PK = ?", pk).
				ScanIndexForward(true).
				LastEvaluatedKey(&lastKey)

			var processedCount int
			err := query.Each(func(item Item) (bool, error) {
				processedCount++
				// Try to unmarshal into wrong type
				var badTarget int
				if err := item.Unmarshal(&badTarget); err != nil {
					return false, err
				}
				return true, nil
			})

			if err == nil {
				t.Fatal("got nil; want error")
			}
			if processedCount != 1 {
				t.Fatalf("processed %d items; want 1", processedCount)
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
