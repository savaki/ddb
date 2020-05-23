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
