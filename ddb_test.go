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
