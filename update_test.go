package ddb

import (
	"testing"
)

type UpdateTable struct {
	ID   string `ddb:"hash_key"`
	Date string `ddb:"range_key"`
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
		update := table.Update(String("hello")).Range(String("world"))

		// When
		update.Set("#a = ?", Int64(123))
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input := update.makeUpdateItemInput()
		assertEqual(t, input, "testdata/update_set_ok.json")
	})

	t.Run("multiple calls to set", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update(String("hello")).Range(String("world"))

		// When
		update.Set("#a = ?", Int64(123))
		update.Set("#b = ?", Int64(456))
		if update.err != nil {
			t.Fatalf("got %v; want nil", update.err)
		}

		input := update.makeUpdateItemInput()
		assertEqual(t, input, "testdata/update_set_multiple.json")
	})

	t.Run("invalid attribute", func(t *testing.T) {
		table := New(nil).MustTable(tableName, UpdateTable{})
		update := table.Update(String("hello")).Range(String("world"))

		// When
		update.Set("#junk = ?", Int64(123))
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

		update := table.Update(String("hello")).Range(String("world"))
		update.Set("#a = ?", Int64(123))
		err := update.Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}
