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
		ctx   = context.Background()
		want  = GetExample{ID: "abc"}
		mock  = &Mock{getItem: want}
		table = New(mock).MustTable("example", GetExample{})
	)

	err := table.Put(want).RunWithContext(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var got GetExample
	err = table.Get(String("abc")).ScanWithContext(ctx, &got)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v; want %#v", got, want)
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

	err = table.Put(want).RunWithContext(ctx)
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
