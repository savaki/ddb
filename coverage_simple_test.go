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
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type SimpleExample struct {
	ID   string `ddb:"hash"`
	Name string
}

// Test ConsumedCapacity.CapacityUnits
func TestConsumedCapacity_CapacityUnits(t *testing.T) {
	readUnits := float64(5)
	writeUnits := float64(3)

	c := &ConsumedCapacity{}
	// First add updates the atomic counters
	c.add(&types.ConsumedCapacity{
		ReadCapacityUnits:  &readUnits,
		WriteCapacityUnits: &writeUnits,
	})
	// Second add updates capacityUnits
	c.add(&types.ConsumedCapacity{
		CapacityUnits: &readUnits,
	})

	got := c.CapacityUnits()
	if got != 5.0 {
		t.Fatalf("got %v; want 5.0", got)
	}
}

// Test Put.ConsumedCapacity
func TestPut_ConsumedCapacity2(t *testing.T) {
	var capacity ConsumedCapacity
	mock := &Mock{
		readUnits:  2,
		writeUnits: 3,
	}
	table := New(mock).MustTable("example", SimpleExample{})

	err := table.Put(SimpleExample{ID: "abc"}).
		ConsumedCapacity(&capacity).
		Run()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if capacity.ReadUnits != 2 {
		t.Fatalf("got %v; want 2", capacity.ReadUnits)
	}
}

// Test Query.ConsumedCapacity
func TestQuery_ConsumedCapacity2(t *testing.T) {
	var capacity ConsumedCapacity
	mock := &Mock{
		readUnits: 5,
		queryItems: []interface{}{
			SimpleExample{ID: "abc"},
		},
	}
	table := New(mock).MustTable("example", SimpleExample{})

	var results []SimpleExample
	err := table.Query("#ID = ?", "abc").
		ConsumedCapacity(&capacity).
		FindAll(&results)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if capacity.ReadUnits != 5 {
		t.Fatalf("got %v; want 5", capacity.ReadUnits)
	}
}

// Test Update.ConsumedCapacity
func TestUpdate_ConsumedCapacity2(t *testing.T) {
	var capacity ConsumedCapacity
	mock := &Mock{
		writeUnits: 2,
	}
	table := New(mock).MustTable("example", SimpleExample{})

	err := table.Update("abc").
		Set("#Name = ?", "test").
		ConsumedCapacity(&capacity).
		Run()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if capacity.WriteUnits != 2 {
		t.Fatalf("got %v; want 2", capacity.WriteUnits)
	}
}

// Test Item.Raw
func TestItem_Raw2(t *testing.T) {
	mock := &Mock{
		scanItems: []interface{}{
			SimpleExample{ID: "abc"},
		},
	}
	table := New(mock).MustTable("example", SimpleExample{})

	var called bool
	err := table.Scan().Each(func(item Item) (bool, error) {
		raw := item.Raw()
		if raw == nil {
			t.Fatal("got nil; want raw map")
		}
		called = true
		return false, nil
	})
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if !called {
		t.Fatal("callback not called")
	}
}

// Test Query.Select
func TestQuery_Select2(t *testing.T) {
	mock := &Mock{
		queryItems: []interface{}{
			SimpleExample{ID: "abc"},
		},
	}
	table := New(mock).MustTable("example", SimpleExample{})

	query := table.Query("#ID = ?", "abc").
		Select(types.SelectSpecificAttributes)

	input, err := query.QueryInput()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if input.Select != types.SelectSpecificAttributes {
		t.Fatalf("got %v; want %v", input.Select, types.SelectSpecificAttributes)
	}
}

// Test error Keys and TableName methods
func TestError_KeysAndTableName2(t *testing.T) {
	hashKey := &types.AttributeValueMemberS{Value: "abc"}
	rangeKey := &types.AttributeValueMemberS{Value: "def"}

	err := notFoundError(hashKey, rangeKey, "test-table")

	gotHash, gotRange := err.Keys()
	if gotHash == nil {
		t.Fatal("got nil hash key")
	}
	if gotRange == nil {
		t.Fatal("got nil range key")
	}

	if err.TableName() != "test-table" {
		t.Fatalf("got %v; want test-table", err.TableName())
	}
}

// Test keyToString with different attribute types
func TestKeyToString2(t *testing.T) {
	tests := []struct {
		name string
		key  types.AttributeValue
		want string
	}{
		{
			name: "binary",
			key:  &types.AttributeValueMemberB{Value: []byte{0x01, 0x02}},
			want: "0102",
		},
		{
			name: "bool (default case)",
			key:  &types.AttributeValueMemberBOOL{Value: true},
			want: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := keyToString(tt.key)
			if got != tt.want {
				t.Fatalf("got %v; want %v", got, tt.want)
			}
		})
	}
}

// Test notFoundError variants
func TestNotFoundError2(t *testing.T) {
	t.Run("no keys", func(t *testing.T) {
		err := notFoundError(nil, nil, "table")
		if err.Message() != "item not found" {
			t.Fatalf("got %v; want 'item not found'", err.Message())
		}
	})

	t.Run("hash key only", func(t *testing.T) {
		hashKey := &types.AttributeValueMemberS{Value: "abc"}
		err := notFoundError(hashKey, nil, "table")
		if err.Message() != "failed to find item, abc, in table, table" {
			t.Fatalf("got %v", err.Message())
		}
	})
}

// Test Get.ScanTx and getTx methods
func TestGet_ScanTx(t *testing.T) {
	mock := &Mock{
		getItem: SimpleExample{ID: "abc", Name: "test"},
	}
	table := New(mock).MustTable("example", SimpleExample{})

	t.Run("ScanTx and Tx", func(t *testing.T) {
		var result SimpleExample
		getTx := table.Get("abc").ScanTx(&result)

		tx, err := getTx.Tx()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if tx == nil {
			t.Fatal("got nil; want tx")
		}
		if tx.Get == nil {
			t.Fatal("got nil Get; want not nil")
		}
	})

	t.Run("Decode with item", func(t *testing.T) {
		var result SimpleExample
		getTx := table.Get("abc").ScanTx(&result)

		item := map[string]types.AttributeValue{
			"ID":   &types.AttributeValueMemberS{Value: "abc"},
			"Name": &types.AttributeValueMemberS{Value: "test"},
		}
		response := &types.ItemResponse{Item: item}

		err := getTx.Decode(response)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if result.ID != "abc" {
			t.Fatalf("got %v; want abc", result.ID)
		}
	})

	t.Run("Decode with empty item", func(t *testing.T) {
		var result SimpleExample
		getTx := table.Get("xyz").ScanTx(&result)

		response := &types.ItemResponse{Item: nil}

		err := getTx.Decode(response)
		if err == nil {
			t.Fatal("got nil; want error")
		}
	})
}

// Test DDB configuration methods
func TestDDB_Config(t *testing.T) {
	mock := &Mock{}
	ddb := New(mock)

	t.Run("WithTokenFunc", func(t *testing.T) {
		fn := func() string {
			return "test-token"
		}
		ddb2 := ddb.WithTokenFunc(fn)
		if ddb2 == nil {
			t.Fatal("got nil; want ddb")
		}
	})

	t.Run("WithTokenFunc nil", func(t *testing.T) {
		ddb2 := ddb.WithTokenFunc(nil)
		if ddb2 == nil {
			t.Fatal("got nil; want ddb")
		}
	})

	t.Run("WithTransactAttempts", func(t *testing.T) {
		ddb2 := ddb.WithTransactAttempts(5)
		if ddb2 == nil {
			t.Fatal("got nil; want ddb")
		}
	})

	t.Run("WithTransactAttempts panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("got no panic; want panic")
			}
		}()
		ddb.WithTransactAttempts(20)
	})

	t.Run("WithTransactTimeout", func(t *testing.T) {
		fn := func(i int) time.Duration {
			return time.Second * time.Duration(i)
		}
		ddb2 := ddb.WithTransactTimeout(fn)
		if ddb2 == nil {
			t.Fatal("got nil; want ddb")
		}
	})

	t.Run("WithTransactTimeout nil", func(t *testing.T) {
		ddb2 := ddb.WithTransactTimeout(nil)
		if ddb2 == nil {
			t.Fatal("got nil; want ddb")
		}
	})
}

// Test TransactGetItems
func TestDDB_TransactGetItems(t *testing.T) {
	mock := &Mock{
		getItem: SimpleExample{ID: "abc", Name: "test"},
	}
	ddb := New(mock)
	table := ddb.MustTable("example", SimpleExample{})

	t.Run("TransactGetItems", func(t *testing.T) {
		var result1, result2 SimpleExample
		getTx1 := table.Get("abc").ScanTx(&result1)
		getTx2 := table.Get("def").ScanTx(&result2)

		err := ddb.TransactGetItems(getTx1, getTx2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("TransactGetItemsWithContext", func(t *testing.T) {
		var result SimpleExample
		getTx := table.Get("abc").ScanTx(&result)

		ctx := context.Background()
		err := ddb.TransactGetItemsWithContext(ctx, getTx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}

// Test Delete.Condition error path
func TestDelete_ConditionError(t *testing.T) {
	mock := &Mock{}
	table := New(mock).MustTable("example", SimpleExample{})

	// Test invalid condition expression causes error
	del := table.Delete("abc").Condition("invalid ??? syntax")
	_, err := del.DeleteItemInput()
	if err == nil {
		t.Fatal("got nil; want error")
	}
}

// Test Put.Condition error path
func TestPut_ConditionError(t *testing.T) {
	mock := &Mock{}
	table := New(mock).MustTable("example", SimpleExample{})

	// Test invalid condition expression causes error
	put := table.Put(SimpleExample{ID: "abc"}).Condition("invalid ??? syntax")
	_, err := put.PutItemInput()
	if err == nil {
		t.Fatal("got nil; want error")
	}
}

// Test Scan methods
func TestScan_Methods(t *testing.T) {
	mock := &Mock{
		scanItems: []interface{}{
			SimpleExample{ID: "abc", Name: "test1"},
			SimpleExample{ID: "def", Name: "test2"},
		},
	}
	table := New(mock).MustTable("example", SimpleExample{})

	t.Run("Scan with Filter", func(t *testing.T) {
		count := 0
		err := table.Scan().
			Filter("#Name = ?", "test1").
			Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("Scan with ConsistentRead", func(t *testing.T) {
		count := 0
		err := table.Scan().
			ConsistentRead(true).
			Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("Scan with IndexName", func(t *testing.T) {
		count := 0
		err := table.Scan().
			IndexName("test-index").
			Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("Scan with TotalSegments", func(t *testing.T) {
		count := 0
		err := table.Scan().
			TotalSegments(2).
			Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("Scan with Debug", func(t *testing.T) {
		var buf []byte
		count := 0
		err := table.Scan().
			Debug(&mockWriter{buf: &buf}).
			Each(func(item Item) (bool, error) {
				count++
				return true, nil
			})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}

type mockWriter struct {
	buf *[]byte
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	*m.buf = append(*m.buf, p...)
	return len(p), nil
}

// Test Update.Condition error path
func TestUpdate_ConditionError(t *testing.T) {
	mock := &Mock{}
	table := New(mock).MustTable("example", SimpleExample{})

	// Test invalid condition expression causes error
	upd := table.Update("abc").
		Set("#Name = ?", "test").
		Condition("invalid ??? syntax")
	_, err := upd.UpdateItemInput()
	if err == nil {
		t.Fatal("got nil; want error")
	}
}

// Test Query.StartToken error paths
func TestQuery_StartToken(t *testing.T) {
	mock := &Mock{
		queryItems: []interface{}{
			SimpleExample{ID: "abc"},
		},
	}
	table := New(mock).MustTable("example", SimpleExample{})

	t.Run("StartToken with empty string", func(t *testing.T) {
		var results []SimpleExample
		err := table.Query("#ID = ?", "abc").
			StartToken("").
			FindAll(&results)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("StartToken with invalid base64", func(t *testing.T) {
		var results []SimpleExample
		err := table.Query("#ID = ?", "abc").
			StartToken("!!!invalid_base64!!!").
			FindAll(&results)
		if err == nil {
			t.Fatal("got nil; want error")
		}
	})

	t.Run("StartToken with invalid JSON", func(t *testing.T) {
		var results []SimpleExample
		invalidJSON := base64.StdEncoding.EncodeToString([]byte("{invalid json"))
		err := table.Query("#ID = ?", "abc").
			StartToken(invalidJSON).
			FindAll(&results)
		if err == nil {
			t.Fatal("got nil; want error")
		}
	})
}

// Test Query.KeyCondition error path
func TestQuery_KeyConditionError(t *testing.T) {
	mock := &Mock{}
	table := New(mock).MustTable("example", SimpleExample{})

	var results []SimpleExample
	err := table.Query("invalid ??? syntax").FindAll(&results)
	if err == nil {
		t.Fatal("got nil; want error")
	}
}

// Test Scan.Filter error path
func TestScan_FilterError(t *testing.T) {
	mock := &Mock{}
	table := New(mock).MustTable("example", SimpleExample{})

	count := 0
	err := table.Scan().
		Filter("invalid ??? syntax").
		Each(func(item Item) (bool, error) {
			count++
			return true, nil
		})
	if err == nil {
		t.Fatal("got nil; want error")
	}
}

// Test StringSet additional methods
func TestStringSet_MoreMethods(t *testing.T) {
	ss := StringSet{"a", "b", "c"}

	t.Run("StringSlice", func(t *testing.T) {
		slice := ss.StringSlice()
		if len(slice) != 3 {
			t.Fatalf("got %v; want 3", len(slice))
		}
	})

	t.Run("Sub", func(t *testing.T) {
		ss2 := StringSet{"b", "d"}
		result := ss.Sub(ss2)
		if len(result) != 2 {
			t.Fatalf("got %v; want 2", len(result))
		}
	})
}

// Test Update.UpdateItemInput error path
func TestUpdateItemInput_SetError(t *testing.T) {
	mock := &Mock{}
	table := New(mock).MustTable("example", SimpleExample{})

	upd := table.Update("abc").Set("invalid ??? syntax")
	_, err := upd.UpdateItemInput()
	if err == nil {
		t.Fatal("got nil; want error")
	}
}

// Test Table error paths
func TestTable_Errors2(t *testing.T) {
	mock := &Mock{}
	ddb := New(mock)

	t.Run("Table with non-struct", func(t *testing.T) {
		_, err := ddb.Table("test", "not a struct")
		if err == nil {
			t.Fatal("got nil; want error")
		}
	})

	t.Run("MustTable panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("got no panic; want panic")
			}
		}()

		ddb.MustTable("test", "not a struct")
	})
}
