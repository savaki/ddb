package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type UpdateTable struct {
	ID   string `ddb:"hash"`
	Date string `ddb:"range"`
	A    string `dynamodbav:"a"`
	B    string `dynamodbav:"b"`
}

func TestReKeys(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		matches := reKeys.FindAllStringSubmatch("hello #world 123", -1)
		if got, want := len(matches), 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := len(matches[0]), 2; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := matches[0][1], "#world"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("simple", func(t *testing.T) {
		matches := reKeys.FindAllStringSubmatch("abc #hello #world", -1)
		if got, want := len(matches), 2; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := len(matches[0]), 2; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := matches[0][1], "#hello"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := len(matches[1]), 2; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := matches[1][1], "#world"; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
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

	t.Run("invalid attribute", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update("hello").Range("world")

		// When
		update.Set("#junk = ?", 123)
		err := update.Run()

		if !IsInvalidFieldNameError(err) {
			t.Fatalf("got %v; want true", err)
		}
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
