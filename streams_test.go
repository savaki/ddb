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
	"testing"
	"time"
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

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			tm := tc.EpochSeconds.Time()
			if got, want := tm.Format(time.RFC3339), tc.Want; got != want {
				t.Fatalf("got %v; want %v", got, want)
			}
		})
	}
}
