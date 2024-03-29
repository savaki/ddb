package ddbtest

import (
	"testing"
)

type Sample struct {
	ID   string
	Name string
}

func TestEventBuilder_Remove(t *testing.T) {
	builder := New().
		Insert(Sample{ID: "1"}).
		Modify(Sample{ID: "2"}, Sample{ID: "2", Name: "New"}).
		Insert(Sample{ID: "3"})

	event, err := builder.Build()
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if got, want := len(event.Records), 3; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}
