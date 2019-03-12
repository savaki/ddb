package ddb

import (
	"io"
	"reflect"
	"testing"
)

type ScanTable struct {
	ID string `ddb:"hash"`
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

func TestScan_ConsistentRead(t *testing.T) {
	s := &Scan{
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
