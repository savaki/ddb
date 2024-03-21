package ddb

import (
	"reflect"
	"regexp"
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

func TestInt64SetContains(t *testing.T) {
	ii := Int64Set{1, 2, 3}
	if got := ii.Contains(1); !got {
		t.Fatalf("got %v; want %v", got, true)
	}
	if got := ii.Contains(4); got {
		t.Fatalf("got %v; want %v", got, false)
	}
}

func TestInt64SetSub(t *testing.T) {
	ii := Int64Set{1, 2, 3, 4}
	if got := ii.Contains(1); !got {
		t.Fatalf("got %v; want %v", got, true)
	}

	updated := ii.Sub(Int64Set{1})
	if got := updated.Contains(1); got {
		t.Fatalf("got %v; want %v", got, false)
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

func TestContains(t *testing.T) {
	ss := StringSet{"a", "b", "c"}
	if got := ss.Contains("a"); !got {
		t.Fatalf("got %v; want %v", got, true)
	}
	if got := ss.Contains("d"); got {
		t.Fatalf("got %v; want %v", got, false)
	}
}

func TestContainsRegexp(t *testing.T) {
	ss := StringSet{"a", "b", "c"}
	if got := ss.ContainsRegexp(regexp.MustCompile(`a`)); !got {
		t.Fatalf("got %v; want %v", got, true)
	}
	if got := ss.ContainsRegexp(regexp.MustCompile(`d`)); got {
		t.Fatalf("got %v; want %v", got, false)
	}
}
