package ddb

import (
	"fmt"
	"reflect"
	"testing"
)

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
