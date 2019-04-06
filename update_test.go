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

		input, err := update.UpdateItemInput()
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

		input, err := update.UpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_delete_ok.json")
	})
}

func TestUpdate_NewValues(t *testing.T) {
	const tableName = "example"

	t.Run("verify input", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").
			Range("world").
			Set("#A = ?", "blah").
			NewValues(&UpdateTable{})

		input, err := update.UpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_new_values_ok.json")
	})

	t.Run("old values", func(t *testing.T) {
		var (
			want = "abc"
			m    = &Mock{
				updateItem: UpdateTable{A: want},
			}
			table     = New(m).MustTable(tableName, UpdateTable{})
			oldValues UpdateTable
		)

		err := table.Update("key").
			OldValues(&oldValues).
			Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got := oldValues.A; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("new values", func(t *testing.T) {
		var (
			want = "abc"
			m    = &Mock{
				updateItem: UpdateTable{A: want},
			}
			table     = New(m).MustTable(tableName, UpdateTable{})
			newValues UpdateTable
		)

		err := table.Update("key").
			NewValues(&newValues).
			Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got := newValues.A; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestUpdate_OldValues(t *testing.T) {
	const tableName = "example"

	t.Run("verify input", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").
			Range("world").
			Set("#A = ?", "blah").
			OldValues(&UpdateTable{})

		input, err := update.UpdateItemInput()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		assertEqual(t, input, "testdata/update_old_values_ok.json")
	})
}

func TestUpdate_BothValues(t *testing.T) {
	const tableName = "example"

	t.Run("ok", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").
			Range("world").
			Set("#A = ?", "blah").
			NewValues(&UpdateTable{}).
			OldValues(&UpdateTable{})

		_, err := update.UpdateItemInput()
		if err == nil {
			t.Fatalf("got nil; want not nil")
		}
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

		input, err := update.UpdateItemInput()
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

		input, err := update.UpdateItemInput()
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

		input, err := update.UpdateItemInput()
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

		input, err := update.UpdateItemInput()
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
