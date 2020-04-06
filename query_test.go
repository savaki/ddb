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
