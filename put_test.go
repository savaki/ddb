package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type PutTable struct {
	ID    string `ddb:"hash"`
	Field string
}

func TestPut_Run(t *testing.T) {
	t.Run("aws err", func(t *testing.T) {
		var (
			want  = awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, "boom", nil)
			mock  = &Mock{err: want}
			db    = New(mock)
			table = db.MustTable("example", PutTable{})
		)

		err := table.Put(PutTable{ID: "abc"}).Run()
		if err != want {
			t.Fatalf("got %v; want %v", err, want)
		}
	})
}

func TestPut_Condition(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		var (
			item  = PutTable{ID: "abc"}
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", PutTable{})
		)

		put := table.Put(item)
		put.Condition("attribute_not_exists(#Field)")
		err := put.Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if mock.putInput == nil {
			t.Fatalf("got nil; want not nil")
		}
		assertEqual(t, mock.putInput, "testdata/put_condition_single.json")
	})

	t.Run("multiple", func(t *testing.T) {
		var (
			item  = PutTable{ID: "abc"}
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", PutTable{})
		)

		put := table.Put(item)
		put.Condition("#Field > ?", 0)
		put.Condition("#Field < ?", 10)
		err := put.Run()
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if mock.putInput == nil {
			t.Fatalf("got nil; want not nil")
		}
		assertEqual(t, mock.putInput, "testdata/put_condition_multiple.json")
	})
}
