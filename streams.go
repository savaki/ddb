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
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// EpochSeconds expresses time in unix seconds
type EpochSeconds int64

// EpochSeconds returns time.Time
func (e EpochSeconds) Time() time.Time {
	return time.Unix(int64(e), 0)
}

// Change represents the change performed
type Change struct {
	// The approximate date and time when the stream record was created, in UNIX
	// epoch time (http://www.epochconverter.com/) format.
	ApproximateCreationDateTime EpochSeconds `json:"ApproximateCreationDateTime,omitempty"`

	// Keys for dynamodb modified dynamodb item
	Keys map[string]*dynamodb.AttributeValue `json:"Keys,omitempty"`

	// NewImage holds dynamodb item AFTER modification
	NewImage map[string]*dynamodb.AttributeValue `json:"NewImage,omitempty"`

	// OldImage holds dynamodb item BEFORE modification
	OldImage map[string]*dynamodb.AttributeValue `json:"OldImage,omitempty"`

	// SequenceNumber of stream record
	SequenceNumber string `json:"SequenceNumber"`

	// SizeBytes contains size of record
	SizeBytes int64 `json:"SizeBytes"`

	// StreamViewType indicates what type of information is being held
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_StreamSpecification.html
	StreamViewType string `json:"StreamViewType"`
}

// Record holds the metadata for the dynamodb change
type Record struct {
	// AWSRegion update occurred within
	AWSRegion string `json:"awsRegion"`
	// Change holds the modification to the dynamodb record
	Change Change `json:"dynamodb"`
	// EventID holds a unique identifier for event
	EventID string `json:"eventID"`
	// EventName will be one of INSERT, MODIFY, or REMOVE
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html
	EventName string `json:"eventName"`
	// EventSource for record.  Will generally be aws:dynamodb
	EventSource string `json:"eventSource"`
	// EventSourceARN holds the arn of the resource that generated the record
	EventSourceARN string `json:"eventSourceARN"`
	// EventVersion number of the stream format
	EventVersion string `json:"eventVersion"`
}

// Event record emitted by dynamodb streams.
//
// Motivation:
// While github.com/aws/aws-lambda-go is a fantastic library for working with lambda in Go,
// the dynamodb type defined in the Record cannot be unmarshaled by
// github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute
//
type Event struct {
	// Records contains the modified records in order
	Records []Record `json:"Records"`
}

var reTableName = regexp.MustCompile(`\d{12}:table/([^/]+)/`)

// TableName returns the table name for a given record
func TableName(eventSourceARN string) (string, bool) {
	match := reTableName.FindStringSubmatch(eventSourceARN)
	if len(match) != 2 {
		return "", false
	}
	return match[1], true
}
