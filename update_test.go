package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type UpdateTable struct {
	ID    string `ddb:"hash"`
	Date  string `ddb:"range"`
	A     string `dynamodbav:"a"`
	B     string `dynamodbav:"b"`
	Count int
}

func TestUpdate_Add(t *testing.T) {
	const tableName = "example"

	t.Run("ok", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").Range("world")

		// When
		update.Add("#Count ?", 1)
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input, err := update.makeUpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_add_ok.json")
	})
}

func TestUpdate_Delete(t *testing.T) {
	const tableName = "example"

	t.Run("ok", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").Range("world")

		// When
		update.Delete("#Count == ?", 1)
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input, err := update.makeUpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_delete_ok.json")
	})
}

func TestUpdate_Remove(t *testing.T) {
	const tableName = "example"

	t.Run("ok", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").Range("world")

		// When
		update.Remove("#Count")
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input, err := update.makeUpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_remove_ok.json")
	})
}

func TestUpdate_Set(t *testing.T) {
	const tableName = "example"

	t.Run("ok", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").Range("world")

		// When
		update.Set("#a = ?", 123)
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input, err := update.makeUpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_set_ok.json")
	})

	t.Run("conditional", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").Range("world")

		// When
		update.Condition("#A == ?", "blah")
		update.Set("#a = ?", 123)
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input, err := update.makeUpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_set_conditional.json")
	})

	t.Run("multiple calls to set", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").Range("world")

		// When
		update.Set("#a = ?", 123)
		update.Set("#b = ?", 456)
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input, err := update.makeUpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_set_multiple.json")
	})
}

func TestUpdate_Run(t *testing.T) {
	const tableName = "example"

	t.Run("ok", func(t *testing.T) {
		var (
			mock  = &Mock{}
			table = New(mock).MustTable(tableName, UpdateTable{})
		)

		update := table.Update("hello").Range("world")
		update.Set("#a = ?", 123)
		err := update.Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("aws err", func(t *testing.T) {
		var (
			original = awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, "boom", nil)
			mock     = &Mock{err: original}
			table    = New(mock).MustTable(tableName, UpdateTable{})
		)

		update := table.Update("hello").Range("world")
		update.Set("#a = ?", 123)
		err := update.Run()
		if err != original {
			t.Fatalf("got %v; want %v", err, original)
		}
	})
}
