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
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type expression struct {
	attributes []*attributeSpec
	Names      map[string]string
	namesRev   map[string]string // reverse lookup: attribute name -> key
	Values     map[string]types.AttributeValue
	index      int64

	Adds       *strings.Builder
	Conditions *strings.Builder
	Deletes    *strings.Builder
	Filters    *strings.Builder
	Removes    *strings.Builder
	Sets       *strings.Builder
}

func newExpression(attributes ...*attributeSpec) *expression {
	return &expression{
		attributes: attributes,
	}
}

// addName adds a name to both Names and namesRev maps and returns the generated key.
// The go:inline directive forces the compiler to inline this function for performance.
//
//go:inline
func (e *expression) addName(name string) string {
	key := "#n" + strconv.Itoa(len(e.Names)+1)
	e.Names[key] = name
	e.namesRev[name] = key
	return key
}

func (e *expression) addExpressionAttributeName(name string) string {
	if e.Names == nil {
		e.Names = make(map[string]string, 4)
		e.namesRev = make(map[string]string, 4)
	}

	// use existing attribute name where possible (O(1) lookup)
	if key, exists := e.namesRev[name]; exists {
		return key
	}

	// Check if name matches an attribute spec
	for _, attr := range e.attributes {
		switch name {
		case attr.AttributeName, attr.FieldName:
			// Check if we already have this attribute name mapped
			if key, exists := e.namesRev[attr.AttributeName]; exists {
				return key
			}
			return e.addName(attr.AttributeName)
		}
	}

	return e.addName(name)
}

func (e *expression) addExpressionAttributeValue(item types.AttributeValue) string {
	if e.Values == nil {
		e.Values = make(map[string]types.AttributeValue, 4)
	}

	id := atomic.AddInt64(&e.index, 1)
	name := ":v" + strconv.FormatInt(id, 10)
	e.Values[name] = item

	return name
}

func (e *expression) UpdateExpression() *string {
	padding := 3
	size := 0
	if e.Adds != nil {
		size += e.Adds.Len() + padding
	}
	if e.Deletes != nil {
		size += e.Deletes.Len() + padding
	}
	if e.Removes != nil {
		size += e.Removes.Len() + padding
	}
	if e.Sets != nil {
		size += e.Sets.Len() + padding
	}

	if size == 0 {
		return nil
	}

	buf := &strings.Builder{}
	buf.Grow(size)

	// ordering as defined here:
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
	first := true
	if e.Sets != nil {
		buf.WriteString(e.Sets.String())
		first = false
	}
	if e.Removes != nil {
		if !first {
			buf.WriteString(" ")
		}
		buf.WriteString(e.Removes.String())
		first = false
	}
	if e.Adds != nil {
		if !first {
			buf.WriteString(" ")
		}
		buf.WriteString(e.Adds.String())
		first = false
	}
	if e.Deletes != nil {
		if !first {
			buf.WriteString(" ")
		}
		buf.WriteString(e.Deletes.String())
	}

	result := buf.String()
	return &result
}

func (e *expression) ConditionExpression() *string {
	if e.Conditions == nil {
		return nil
	}

	result := e.Conditions.String()
	return &result
}

func (e *expression) FilterExpression() *string {
	if e.Filters == nil {
		return nil
	}

	result := e.Filters.String()
	return &result
}

func (e *expression) append(buf *strings.Builder, keyword, separator, expr string, values ...interface{}) error {
	expr, err := e.parse(expr, values...)
	if err != nil {
		return err
	}

	// Trim leading/trailing space inline to avoid allocation
	start := 0
	end := len(expr)
	for start < end && expr[start] == ' ' {
		start++
	}
	for end > start && expr[end-1] == ' ' {
		end--
	}

	if buf.Len() == 0 {
		if len(keyword) > 0 {
			buf.WriteString(keyword)
			buf.WriteString(" ")
		}
	} else {
		buf.WriteString(separator)
	}
	buf.WriteString(expr[start:end])

	return nil
}

const comma = ", "

func (e *expression) Add(expr string, values ...interface{}) error {
	if e.Adds == nil {
		e.Adds = &strings.Builder{}
		e.Adds.Grow(128)
	}
	return e.append(e.Adds, "Add", comma, expr, values...)
}

func (e *expression) Condition(expr string, values ...interface{}) error {
	if e.Conditions == nil {
		e.Conditions = &strings.Builder{}
		e.Conditions.Grow(128)
	}
	return e.append(e.Conditions, "", " and ", expr, values...)
}

func (e *expression) Delete(expr string, values ...interface{}) error {
	if e.Deletes == nil {
		e.Deletes = &strings.Builder{}
		e.Deletes.Grow(128)
	}
	return e.append(e.Deletes, "Delete", comma, expr, values...)
}

func (e *expression) Filter(expr string, values ...interface{}) error {
	if e.Filters == nil {
		e.Filters = &strings.Builder{}
		e.Filters.Grow(128)
	}
	return e.append(e.Filters, "", " and ", expr, values...)
}

func (e *expression) Remove(expr string, values ...interface{}) error {
	if e.Removes == nil {
		e.Removes = &strings.Builder{}
		e.Removes.Grow(128)
	}
	return e.append(e.Removes, "Remove", comma, expr, values...)
}

func (e *expression) Set(expr string, values ...interface{}) error {
	if e.Sets == nil {
		e.Sets = &strings.Builder{}
		e.Sets.Grow(128)
	}
	return e.append(e.Sets, "Set", comma, expr, values...)
}

func (e *expression) parse(expr string, values ...interface{}) (string, error) {
	var (
		inName  bool
		index   int
		buf     = &strings.Builder{}
		bufName = &strings.Builder{}
	)

	buf.Grow(len(expr) * 2)
	for _, v := range expr {
		if inName {
			if isAlphaNumeric(v) {
				bufName.WriteRune(v)
				continue

			} else if v == '?' {
				if index >= len(values) {
					return "", errorf(ErrMismatchedValueCount, "not enough values")
				}

				name, ok := values[index].(string)
				if !ok {
					return "", fmt.Errorf("expected value[%v] to be a string", index)
				}
				index++

				key := e.addExpressionAttributeName(name)
				buf.WriteString(key)

				inName = false
				bufName.Reset()
				continue

			} else {
				key := e.addExpressionAttributeName(bufName.String())
				buf.WriteString(key)
				inName = false
				bufName.Reset()
			}
		}

		switch v {
		case '?':
			if index >= len(values) {
				return "", errorf(ErrMismatchedValueCount, "not enough values")
			}

			item, err := marshal(values[index])
			if err != nil {
				return "", fmt.Errorf("unable to marshal value: %v", err)
			}
			index++

			key := e.addExpressionAttributeValue(item)
			buf.WriteString(key)

		case '#':
			inName = true
			bufName.Reset()

		default:
			buf.WriteRune(v)
		}
	}

	if bufName.Len() > 0 {
		key := e.addExpressionAttributeName(bufName.String())
		buf.WriteString(key)
	}

	if got, want := len(values), index; got != want {
		return "", fmt.Errorf("mismatched number of values; got %v, want %v", got, want)
	}

	return buf.String(), nil
}

func isAlphaNumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
