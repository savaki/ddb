package ddb

import (
	"io"
	"testing"
)

func TestIsItemNotFoundError(t *testing.T) {
	err := errorf(ErrItemNotFound, "boom")

	if !IsItemNotFoundError(err) {
		t.Fatalf("got false, want true")
	}
	if !IsItemNotFoundError(wrapf(err, "Wrapped", "boom")) {
		t.Fatalf("got false, want true")
	}
	if IsItemNotFoundError(nil) {
		t.Fatalf("got true, want false")
	}
	if IsItemNotFoundError(io.EOF) {
		t.Fatalf("got true, want false")
	}
	if IsItemNotFoundError(wrapf(nil, "NilError", "boom")) {
		t.Fatalf("got true, want false")
	}
}

func TestBaseError_Error(t *testing.T) {
	err := errorf(ErrItemNotFound, "boom")
	wrapped := wrapf(err, "Wrapper", "pow")

	if got, want := err.Error(), "ItemNotFound: boom"; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
	if got, want := wrapped.Error(), "Wrapper: pow: ItemNotFound: boom"; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestBaseError_Message(t *testing.T) {
	err := errorf(ErrItemNotFound, "boom")

	if got, want := err.Message(), "boom"; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestIsMismatchedValueCount(t *testing.T) {
	table := New(nil).MustTable("example", UpdateTable{})
	update := table.Update(String("hello"))
	update.Set("#a = ?, #b = ?", Int64(123))
	err := update.Run()
	if !IsMismatchedValueCountError(err) {
		t.Fatalf("got %v; want MismatchedValueCount", err)
	}
}
