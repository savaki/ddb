package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func TestDDB_TransactWriteItems(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("blah", Example{})
			item  = table.Delete("abc").
				ReturnValuesOnConditionCheckFailure(dynamodb.ReturnValuesOnConditionCheckFailureNone)
		)

		db.tokenFunc = func() string { return "def" }

		_, err := db.TransactWriteItems(item)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		assertEqual(t, mock.writeInput, "testdata/tx_delete_ok.json")
	})

	t.Run("put", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("blah", Example{})
			item  = table.Put(Example{
				ID:   "abc",
				Name: "blah",
			}).ReturnValuesOnConditionCheckFailure(dynamodb.ReturnValuesOnConditionCheckFailureNone)
		)

		db.tokenFunc = func() string { return "def" }

		_, err := db.TransactWriteItems(item)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		assertEqual(t, mock.writeInput, "testdata/tx_put_ok.json")
	})

	t.Run("update", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("blah", Example{})
			item  = table.Update("abc").Set("#Name = ?", "def").
				ReturnValuesOnConditionCheckFailure(dynamodb.ReturnValuesOnConditionCheckFailureNone)
		)

		db.tokenFunc = func() string { return "def" }

		_, err := db.TransactWriteItems(item)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		assertEqual(t, mock.writeInput, "testdata/tx_update_ok.json")
	})
}

func Test_makeRequestToken(t *testing.T) {
	token := makeRequestToken()
	if token == "" {
		t.Fatalf("got blank; want not blank")
	}
}
