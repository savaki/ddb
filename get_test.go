package ddb

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type GetExample struct {
	ID string `ddb:"hash_key"`
}

func TestGet_One(t *testing.T) {
	var (
		want  = GetExample{ID: "abc"}
		mock  = &Mock{getItem: want}
		table = New(mock).MustTable("example", GetExample{})
	)

	err := table.Put(want).Run()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var got GetExample
	err = table.Get(String("abc")).Scan(&got)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v; want %#v", got, want)
	}

	consumed := table.ConsumedCapacity()
	if got, want := consumed.ReadUnits, int64(1); got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
	if got, want := consumed.WriteUnits, int64(1); got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestLive(t *testing.T) {
	if !runIntegrationTests {
		t.SkipNow()
	}

	var (
		ctx  = context.Background()
		s, _ = session.NewSession(aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
			WithRegion("us-west-2").
			WithEndpoint("http://localhost:8000"))
		api       = dynamodb.New(s)
		tableName = fmt.Sprintf("tmp-%v", time.Now().UnixNano())
		table     = New(api).MustTable(tableName, GetExample{})
		want      = GetExample{ID: "abc"}
	)

	err := table.CreateTableIfNotExists(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	err = table.Put(want).Run()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var got GetExample
	err = table.Get(String(want.ID)).ScanWithContext(ctx, &got)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestInt64Value(t *testing.T) {
	value := Int64(123)
	if value.item == nil {
		t.Fatalf("got nil; want not nil")
	}
	if got, want := *value.item.N, "123"; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestGet_Range(t *testing.T) {
	want := "abc"
	g := &Get{}
	g.Range(String(want))

	if got := g.rangeKey.item; got == nil {
		t.Fatalf("got nil; want not nil")
	}
	if got := *g.rangeKey.item.S; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}
