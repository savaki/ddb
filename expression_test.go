package ddb

import (
	"testing"
)

func TestParse(t *testing.T) {
	testCases := map[string]struct {
		Expr     string
		Values   []interface{}
		Want     string
		Filename string
	}{
		"nop": {
			Expr:     "hello",
			Want:     "hello",
			Filename: "testdata/parse/nop.json",
		},
		"one value": {
			Expr: "hello = ?",
			Want: "hello = :v1",
			Values: []interface{}{
				"world",
			},
			Filename: "testdata/parse/one_value.json",
		},
		"two values": {
			Expr: "a = ?, b = ?",
			Want: "a = :v1, b = :v2",
			Values: []interface{}{
				"aa",
				"bb",
			},
			Filename: "testdata/parse/two_values.json",
		},
		"dynamic name": {
			Expr: "#? = 1",
			Values: []interface{}{
				"hello",
			},
			Want:     "#n1 = 1",
			Filename: "testdata/parse/dynamic_name.json",
		},
		"custom name": {
			Expr:     "#custom = 1",
			Want:     "#n1 = 1",
			Filename: "testdata/parse/custom_name.json",
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			expr := &expression{}
			got, err := expr.parse(tc.Expr, tc.Values...)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}
			if got != tc.Want {
				t.Fatalf("got %v; want %v", got, tc.Want)
			}
			assertEqual(t, expr, tc.Filename)
		})
	}

	t.Run("too many args", func(t *testing.T) {
		expr := &expression{}
		_, err := expr.parse("a = b ", "not-used")
		if err == nil {
			t.Fatal("got nil; want not nil")
		}
	})

	t.Run("not enough names", func(t *testing.T) {
		expr := &expression{}
		_, err := expr.parse("#? = 1")
		if err == nil {
			t.Fatal("got nil; want not nil")
		}
	})

	t.Run("name not a string", func(t *testing.T) {
		expr := &expression{}
		_, err := expr.parse("#? = 1", 123)
		if err == nil {
			t.Fatal("got nil; want not nil")
		}
	})

	t.Run("not enough values", func(t *testing.T) {
		expr := &expression{}
		_, err := expr.parse("a = ?")
		if err == nil {
			t.Fatal("got nil; want not nil")
		}
	})
}
