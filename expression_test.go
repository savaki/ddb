// Copyright 2020 Matt Ho
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

func Test_expression_FilterExpression(t *testing.T) {
	tests := []struct {
		name   string
		expr   string
		values []interface{}
		want   string
	}{
		{
			name: "simple",
			expr: "a > b",
			want: "a > b",
		},
		{
			name:   "single arg",
			expr:   "a > ?",
			values: []interface{}{1},
			want:   "a > :v1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &expression{}
			err := expr.Filter(tt.expr, tt.values...)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			got := expr.FilterExpression()
			if *got != tt.want {
				t.Fatalf("got %v; want %v", *got, tt.want)
			}
		})
	}
}
