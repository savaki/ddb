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
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

func TestTableName(t *testing.T) {
	testCases := map[string]struct {
		EventSourceARN string
		Want           string
		WantOk         bool
	}{
		"blank": {
			EventSourceARN: "",
			Want:           "",
			WantOk:         false,
		},
		"sample": {
			EventSourceARN: "arn:aws:dynamodb:us-east-1:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
			Want:           "BarkTable",
			WantOk:         true,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			got, ok := TableName(tc.EventSourceARN)
			if ok != tc.WantOk {
				t.Fatalf("got %v; want %v", ok, tc.WantOk)
			}
			if got != tc.Want {
				t.Fatalf("got %v; want %v", got, tc.Want)
			}
		})
	}
}

func TestEpochSeconds_Time(t *testing.T) {
	testCases := map[string]struct {
		EpochSeconds EpochSeconds
		Want         string
	}{
		"zero": {
			EpochSeconds: 0,
			Want:         "1969-12-31T18:00:00-06:00",
		},
		"sample": {
			EpochSeconds: 1590277509,
			Want:         "2020-05-23T18:45:09-05:00",
		},
	}

	loc, _ := time.LoadLocation("US/Central")
	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			tm := tc.EpochSeconds.Time().In(loc)
			if got, want := tm.Format(time.RFC3339), tc.Want; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
		})
	}
}

func TestEpochSeconds_JSON(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		want := EpochSeconds(123)
		data, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		var got EpochSeconds
		err = json.Unmarshal(data, &got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		if got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type T struct {
			Unix EpochSeconds
		}

		const text = `{"Unix":1.59041719E9}`
		var got T
		err := json.Unmarshal([]byte(text), &got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if want := EpochSeconds(1590417190); got.Unix != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("null", func(t *testing.T) {
		type T struct {
			Unix EpochSeconds
		}

		const text = `{"Unix":null}`
		var got T
		err := json.Unmarshal([]byte(text), &got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if want := EpochSeconds(0); got.Unix != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestEvent_Unmarshal(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{
			name:     "insert",
			filename: "testdata/streams/insert_event.json",
		},
		{
			name:     "modify",
			filename: "testdata/streams/modify_event.json",
		},
		{
			name:     "remove",
			filename: "testdata/streams/remove_event.json",
		},
		{
			name:     "multiple_records",
			filename: "testdata/streams/multiple_records.json",
		},
		{
			name:     "keys_only",
			filename: "testdata/streams/keys_only_event.json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(tc.filename)
			if err != nil {
				t.Fatalf("failed to read file: %v", err)
			}

			var event Event
			err = json.Unmarshal(data, &event)
			if err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}

			if len(event.Records) == 0 {
				t.Fatal("expected at least one record")
			}

			// Verify basic structure
			for i, record := range event.Records {
				if record.EventID == "" {
					t.Errorf("record %d: missing EventID", i)
				}
				if record.EventName == "" {
					t.Errorf("record %d: missing EventName", i)
				}
				if record.EventSource == "" {
					t.Errorf("record %d: missing EventSource", i)
				}
				if record.AWSRegion == "" {
					t.Errorf("record %d: missing AWSRegion", i)
				}
				if record.Change.SequenceNumber == "" {
					t.Errorf("record %d: missing SequenceNumber", i)
				}
			}
		})
	}
}

func TestImage_UnmarshalMap(t *testing.T) {
	t.Run("unmarshal NewImage from insert event", func(t *testing.T) {
		data, err := os.ReadFile("testdata/streams/insert_event.json")
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}

		var event Event
		err = json.Unmarshal(data, &event)
		if err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}

		if len(event.Records) == 0 {
			t.Fatal("expected at least one record")
		}

		// Test unmarshaling into a struct
		type TestItem struct {
			Id      int    `dynamodbav:"Id"`
			Message string `dynamodbav:"Message"`
			Status  string `dynamodbav:"Status"`
		}

		var item TestItem
		err = attributevalue.UnmarshalMap(event.Records[0].Change.NewImage, &item)
		if err != nil {
			t.Fatalf("failed to unmarshal map: %v", err)
		}

		// Verify the unmarshaled values
		if item.Id != 101 {
			t.Errorf("expected Id = 101, got %d", item.Id)
		}
		if item.Message != "New item!" {
			t.Errorf("expected Message = 'New item!', got '%s'", item.Message)
		}
		if item.Status != "active" {
			t.Errorf("expected Status = 'active', got '%s'", item.Status)
		}
	})

	t.Run("unmarshal OldImage from modify event", func(t *testing.T) {
		data, err := os.ReadFile("testdata/streams/modify_event.json")
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}

		var event Event
		err = json.Unmarshal(data, &event)
		if err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}

		type TestItem struct {
			Id      int    `dynamodbav:"Id"`
			Message string `dynamodbav:"Message"`
			Status  string `dynamodbav:"Status"`
		}

		var oldItem TestItem
		err = attributevalue.UnmarshalMap(event.Records[0].Change.OldImage, &oldItem)
		if err != nil {
			t.Fatalf("failed to unmarshal old image: %v", err)
		}

		if oldItem.Message != "New item!" {
			t.Errorf("expected OldImage Message = 'New item!', got '%s'", oldItem.Message)
		}

		var newItem TestItem
		err = attributevalue.UnmarshalMap(event.Records[0].Change.NewImage, &newItem)
		if err != nil {
			t.Fatalf("failed to unmarshal new image: %v", err)
		}

		if newItem.Message != "This item has changed" {
			t.Errorf("expected NewImage Message = 'This item has changed', got '%s'", newItem.Message)
		}
		if newItem.Status != "updated" {
			t.Errorf("expected NewImage Status = 'updated', got '%s'", newItem.Status)
		}
	})

	t.Run("unmarshal Keys", func(t *testing.T) {
		data, err := os.ReadFile("testdata/streams/keys_only_event.json")
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}

		var event Event
		err = json.Unmarshal(data, &event)
		if err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}

		type KeyStruct struct {
			UserId    string `dynamodbav:"UserId"`
			Timestamp int64  `dynamodbav:"Timestamp"`
		}

		var keys KeyStruct
		err = attributevalue.UnmarshalMap(event.Records[0].Change.Keys, &keys)
		if err != nil {
			t.Fatalf("failed to unmarshal keys: %v", err)
		}

		if keys.UserId != "user123" {
			t.Errorf("expected UserId = 'user123', got '%s'", keys.UserId)
		}
		if keys.Timestamp != 1590277509 {
			t.Errorf("expected Timestamp = 1590277509, got %d", keys.Timestamp)
		}
	})
}
