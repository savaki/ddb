package ddb

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func TestInt64Set(t *testing.T) {
	want := Int64Set{1, 2, 3}

	item, err := dynamodbattribute.Marshal(want)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var got Int64Set
	err = dynamodbattribute.Unmarshal(item, &got)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestStringSet(t *testing.T) {
	want := StringSet{"a", "b", "c"}

	item, err := dynamodbattribute.Marshal(want)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var got StringSet
	err = dynamodbattribute.Unmarshal(item, &got)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v; want %v", got, want)
	}
}
