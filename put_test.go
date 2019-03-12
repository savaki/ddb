package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type PutTable struct {
	ID string `ddb:"hash_key"`
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
