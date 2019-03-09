package ddb

import (
	"testing"
)

type Simple struct {
	Hash  string `ddb:"hash_key"`
	Range string `ddb:"range_key"`
}

func Test_makeKey(t *testing.T) {
	spec, err := inspect("simple", Simple{})
	if err != nil {
		t.Fatalf("got %#v; want nil", err)
	}

	item := makeKey(spec, String("abc"), String("def"))
	assertEqual(t, item, "testdata/keys.json")
}
