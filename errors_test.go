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
	update := table.Update("hello")
	update.Set("#a = ?, #b = ?", 123)
	err := update.Run()
	if !IsMismatchedValueCountError(err) {
		t.Fatalf("got %v; want MismatchedValueCount", err)
	}
}

func TestUnwrap(t *testing.T) {
	want := io.EOF
	err := baseError{
		cause: want,
	}
	got := err.Unwrap()
	if got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestIsInvalidFieldNameError(t *testing.T) {
	got := IsInvalidFieldNameError(&baseError{
		code: ErrInvalidFieldName,
	})
	if got != true {
		t.Fatalf("got false; want true")
	}
}
