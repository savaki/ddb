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

// Benchmarks for DynamoDB expression assembly.
//
// Run all expression benchmarks:
//   go test -bench=BenchmarkExpression -benchmem -run=^$
//
// Run a specific benchmark:
//   go test -bench=BenchmarkExpression_Parse -benchmem -run=^$
//
// Compare before/after performance:
//   go test -bench=BenchmarkExpression -benchmem -run=^$ > old.txt
//   # make changes
//   go test -bench=BenchmarkExpression -benchmem -run=^$ > new.txt
//   benchcmp old.txt new.txt

// BenchmarkExpression_Parse benchmarks the core parsing functionality
func BenchmarkExpression_Parse(b *testing.B) {
	benchmarks := []struct {
		name   string
		expr   string
		values []interface{}
	}{
		{
			name: "simple_literal",
			expr: "a = b",
		},
		{
			name:   "single_value",
			expr:   "a = ?",
			values: []interface{}{"value1"},
		},
		{
			name:   "multiple_values",
			expr:   "a = ? and b = ? and c = ?",
			values: []interface{}{"value1", "value2", "value3"},
		},
		{
			name:   "single_dynamic_name",
			expr:   "#? = ?",
			values: []interface{}{"fieldName", "value1"},
		},
		{
			name: "static_attribute_name",
			expr: "#Status = ?",
			values: []interface{}{"active"},
		},
		{
			name:   "complex_expression",
			expr:   "#? = ? and #? > ? and #? between ? and ?",
			values: []interface{}{"status", "active", "count", 10, "date", "2024-01-01", "2024-12-31"},
		},
		{
			name:   "multiple_static_names",
			expr:   "#Status = ? and #Count > ? and #Type = ?",
			values: []interface{}{"active", 100, "premium"},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				expr := newExpression()
				_, err := expr.parse(bm.expr, bm.values...)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExpression_Condition benchmarks condition expression assembly
func BenchmarkExpression_Condition(b *testing.B) {
	benchmarks := []struct {
		name       string
		conditions []struct {
			expr   string
			values []interface{}
		}
	}{
		{
			name: "single_condition",
			conditions: []struct {
				expr   string
				values []interface{}
			}{
				{expr: "#Status = ?", values: []interface{}{"active"}},
			},
		},
		{
			name: "two_conditions",
			conditions: []struct {
				expr   string
				values []interface{}
			}{
				{expr: "#Status = ?", values: []interface{}{"active"}},
				{expr: "#Count > ?", values: []interface{}{10}},
			},
		},
		{
			name: "five_conditions",
			conditions: []struct {
				expr   string
				values []interface{}
			}{
				{expr: "#Status = ?", values: []interface{}{"active"}},
				{expr: "#Count > ?", values: []interface{}{10}},
				{expr: "#Type = ?", values: []interface{}{"premium"}},
				{expr: "#Date between ? and ?", values: []interface{}{"2024-01-01", "2024-12-31"}},
				{expr: "#Category in (?, ?, ?)", values: []interface{}{"A", "B", "C"}},
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				expr := newExpression()
				for _, cond := range bm.conditions {
					if err := expr.Condition(cond.expr, cond.values...); err != nil {
						b.Fatal(err)
					}
				}
				_ = expr.ConditionExpression()
			}
		})
	}
}

// BenchmarkExpression_Filter benchmarks filter expression assembly
func BenchmarkExpression_Filter(b *testing.B) {
	benchmarks := []struct {
		name    string
		filters []struct {
			expr   string
			values []interface{}
		}
	}{
		{
			name: "single_filter",
			filters: []struct {
				expr   string
				values []interface{}
			}{
				{expr: "#Status = ?", values: []interface{}{"active"}},
			},
		},
		{
			name: "multiple_filters",
			filters: []struct {
				expr   string
				values []interface{}
			}{
				{expr: "#Status = ?", values: []interface{}{"active"}},
				{expr: "#Count > ?", values: []interface{}{10}},
				{expr: "#Type = ?", values: []interface{}{"premium"}},
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				expr := newExpression()
				for _, filter := range bm.filters {
					if err := expr.Filter(filter.expr, filter.values...); err != nil {
						b.Fatal(err)
					}
				}
				_ = expr.FilterExpression()
			}
		})
	}
}

// BenchmarkExpression_Update benchmarks update expression assembly
func BenchmarkExpression_Update(b *testing.B) {
	benchmarks := []struct {
		name       string
		operations []struct {
			typ    string // "set", "add", "remove", "delete"
			expr   string
			values []interface{}
		}
	}{
		{
			name: "single_set",
			operations: []struct {
				typ    string
				expr   string
				values []interface{}
			}{
				{typ: "set", expr: "#Status = ?", values: []interface{}{"active"}},
			},
		},
		{
			name: "multiple_sets",
			operations: []struct {
				typ    string
				expr   string
				values []interface{}
			}{
				{typ: "set", expr: "#Status = ?", values: []interface{}{"active"}},
				{typ: "set", expr: "#Count = ?", values: []interface{}{100}},
				{typ: "set", expr: "#UpdatedAt = ?", values: []interface{}{"2024-01-01"}},
			},
		},
		{
			name: "mixed_operations",
			operations: []struct {
				typ    string
				expr   string
				values []interface{}
			}{
				{typ: "set", expr: "#Status = ?", values: []interface{}{"active"}},
				{typ: "add", expr: "#Count ?", values: []interface{}{1}},
				{typ: "remove", expr: "#TempField", values: nil},
				{typ: "delete", expr: "#Tags ?", values: []interface{}{"oldTag"}},
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				expr := newExpression()
				for _, op := range bm.operations {
					var err error
					switch op.typ {
					case "set":
						err = expr.Set(op.expr, op.values...)
					case "add":
						err = expr.Add(op.expr, op.values...)
					case "remove":
						err = expr.Remove(op.expr, op.values...)
					case "delete":
						err = expr.Delete(op.expr, op.values...)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				_ = expr.UpdateExpression()
			}
		})
	}
}

// BenchmarkExpression_AddExpressionAttributeName benchmarks attribute name management
func BenchmarkExpression_AddExpressionAttributeName(b *testing.B) {
	benchmarks := []struct {
		name  string
		names []string
	}{
		{
			name:  "single_name",
			names: []string{"Status"},
		},
		{
			name:  "five_names",
			names: []string{"Status", "Count", "Type", "Date", "Category"},
		},
		{
			name:  "ten_names",
			names: []string{"Field1", "Field2", "Field3", "Field4", "Field5", "Field6", "Field7", "Field8", "Field9", "Field10"},
		},
		{
			name:  "five_names_with_reuse",
			names: []string{"Status", "Count", "Status", "Type", "Count"}, // Status and Count appear twice
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				expr := newExpression()
				for _, name := range bm.names {
					_ = expr.addExpressionAttributeName(name)
				}
			}
		})
	}
}

// BenchmarkExpression_AddExpressionAttributeValue benchmarks attribute value management
func BenchmarkExpression_AddExpressionAttributeValue(b *testing.B) {
	benchmarks := []struct {
		name   string
		values []interface{}
	}{
		{
			name:   "single_value",
			values: []interface{}{"value1"},
		},
		{
			name:   "five_values",
			values: []interface{}{"value1", "value2", "value3", "value4", "value5"},
		},
		{
			name:   "ten_values",
			values: []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name:   "mixed_types",
			values: []interface{}{"string", 42, true, 3.14, "another"},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				expr := newExpression()
				for _, val := range bm.values {
					item, err := marshal(val)
					if err != nil {
						b.Fatal(err)
					}
					_ = expr.addExpressionAttributeValue(item)
				}
			}
		})
	}
}

// BenchmarkExpression_Complete benchmarks complete expression assembly workflow
func BenchmarkExpression_Complete(b *testing.B) {
	b.Run("query_with_filter", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			expr := newExpression()
			_ = expr.Filter("#Status = ?", "active")
			_ = expr.Filter("#Count > ?", 10)
			_ = expr.Filter("#Type = ?", "premium")
			_ = expr.FilterExpression()
		}
	})

	b.Run("conditional_update", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			expr := newExpression()
			_ = expr.Set("#Status = ?", "active")
			_ = expr.Set("#UpdatedAt = ?", "2024-01-01")
			_ = expr.Add("#Count ?", 1)
			_ = expr.Condition("attribute_exists(#ID)", "ID")
			_ = expr.UpdateExpression()
			_ = expr.ConditionExpression()
		}
	})

	b.Run("complex_update_with_all_operations", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			expr := newExpression()
			_ = expr.Set("#Status = ?", "active")
			_ = expr.Set("#UpdatedAt = ?", "2024-01-01")
			_ = expr.Add("#ViewCount ?", 1)
			_ = expr.Remove("#TempField")
			_ = expr.Delete("#Tags ?", "deprecated")
			_ = expr.Condition("#Version = ?", "Version", 5)
			_ = expr.UpdateExpression()
			_ = expr.ConditionExpression()
		}
	})
}
