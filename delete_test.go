// Copyright 2019 Matt Ho
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

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DeleteTable struct {
	ID    string `ddb:"hash"`
	Date  string `ddb:"range"`
	Field int
}

func TestDelete_Condition(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		var (
			item  = DeleteTable{ID: "abc", Date: "2006-01-02"}
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", DeleteTable{})
		)

		del := table.Delete(item.ID).Range(item.Date)
		del.Condition("#ID != ?", "def")
		err := del.Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if mock.deleteInput == nil {
			t.Fatalf("got nil; want not nil")
		}
		assertEqual(t, mock.deleteInput, "testdata/delete_condition_single.json")
	})

	t.Run("multiple", func(t *testing.T) {
		var (
			item  = DeleteTable{ID: "abc"}
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", DeleteTable{})
		)

		del := table.Delete(item.ID)
		del.Condition("#Field > ?", 0)
		del.Condition("#Field < ?", 10)
		err := del.Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if mock.deleteInput == nil {
			t.Fatalf("got nil; want not nil")
		}
		assertEqual(t, mock.deleteInput, "testdata/delete_condition_multiple.json")
	})

	t.Run("aws err", func(t *testing.T) {
		var (
			original = awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, "boom", nil)
			mock     = &Mock{err: original}
			db       = New(mock)
			table    = db.MustTable("example", DeleteTable{})
		)

		del := table.Delete("blah")
		err := del.Run()
		if err != original {
			t.Fatalf("got %v; want %v", err, original)
		}
	})
}

func TestDelete_ConsumedCapacity(t *testing.T) {
	var (
		mock = &Mock{
			readUnits:  2,
			writeUnits: 3,
		}
		db       = New(mock)
		table    = db.MustTable("example", DeleteTable{})
		capacity ConsumedCapacity
	)

	del := table.Delete("blah").ConsumedCapacity(&capacity)
	err := del.Run()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if got, want := capacity.ReadUnits, mock.readUnits; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
	if got, want := capacity.WriteUnits, mock.writeUnits; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}
