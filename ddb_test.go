package ddb

import (
	"testing"
)

func TestConsumedCapacity_add(t *testing.T) {
	t.Run("handles nil", func(t *testing.T) {
		c := &ConsumedCapacity{}
		c.add(nil)
	})
}

func TestTable_DDB(t *testing.T) {
	var (
		mock  = &Mock{}
		db    = New(mock)
		table = db.MustTable("blah", Example{})
	)

	if got, want := table.DDB(), db; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

type InvalidModel struct {
	ID struct{} `ddb:"hash"` // struct{} is an invalid key
}

func TestTable_ConsumedCapacity(t *testing.T) {
	db := New(nil)
	_, err := db.Table("blah", InvalidModel{})
	if err == nil {
		t.Fatalf("got nil; want not nil")
	}
}
