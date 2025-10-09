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
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// DynamoDB Streams event name constants (re-exported from AWS SDK for convenience)
const (
	EventNameInsert = string(streamtypes.OperationTypeInsert)
	EventNameModify = string(streamtypes.OperationTypeModify)
	EventNameRemove = string(streamtypes.OperationTypeRemove)
)

// EpochSeconds represents a timestamp as Unix seconds (seconds since January 1, 1970 UTC).
// This type is used for DynamoDB stream timestamps.
type EpochSeconds int64

// MarshalJSON marshals EpochSeconds to JSON as a floating-point number.
// This implements the json.Marshaler interface.
func (e EpochSeconds) MarshalJSON() ([]byte, error) {
	return json.Marshal(float64(e))
}

// UnmarshalJSON unmarshals JSON into EpochSeconds from a floating-point number.
// This implements the json.Unmarshaler interface.
func (e *EpochSeconds) UnmarshalJSON(data []byte) error {
	var v float64
	if err := json.Unmarshal(data, &v); err != nil {
		return nil
	}
	*e = EpochSeconds(v)
	return nil
}

// Time converts EpochSeconds to a time.Time value.
func (e EpochSeconds) Time() time.Time {
	return time.Unix(int64(e), 0)
}

// Window represents a tumbling window for Lambda stream processing.
// See https://aws.amazon.com/blogs/compute/using-aws-lambda-for-streaming-analytics/
type Window struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

// Change represents a modification to a DynamoDB item, including the keys,
// old image, and new image of the item.
type Change struct {
	// The approximate date and time when the stream record was created, in UNIX
	// epoch time (http://www.epochconverter.com/) format.
	ApproximateCreationDateTime EpochSeconds `json:"ApproximateCreationDateTime,omitempty"`

	// Keys for dynamodb modified dynamodb item
	Keys map[string]types.AttributeValue `json:"Keys,omitempty"`

	// NewImage holds dynamodb item AFTER modification
	NewImage map[string]types.AttributeValue `json:"NewImage,omitempty"`

	// OldImage holds dynamodb item BEFORE modification
	OldImage map[string]types.AttributeValue `json:"OldImage,omitempty"`

	// SequenceNumber of stream record
	SequenceNumber string `json:"SequenceNumber"`

	// SizeBytes contains size of record
	SizeBytes int64 `json:"SizeBytes"`

	// StreamViewType indicates what type of information is being held
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_StreamSpecification.html
	StreamViewType string `json:"StreamViewType"`
}

// Record represents a single DynamoDB stream record containing metadata
// about a change to an item in the table.
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

// Event represents a DynamoDB stream event that can be processed by AWS Lambda.
// This type addresses the incompatibility between github.com/aws/aws-lambda-go
// and github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue, allowing
// DynamoDB records to be properly unmarshaled.
type Event struct {
	// EventSourceARN holds the arn of the resource that generated the record
	EventSourceARN string `json:"eventSourceARN,omitempty"`

	// IsFinalInvokeForWindow - indicates if this is the last invocation for the tumbling window. This only occurs once per window period. [Tumbling Window]
	IsFinalInvokeForWindow bool `json:"isFinalInvokeForWindow,omitempty"`

	// IsWindowTerminatedEarly - a window ends early only if the state exceeds the maximum allowed size of 1 MB [Tumbling Window]
	IsWindowTerminatedEarly bool `json:"isWindowTerminatedEarly,omitempty"`

	// Records contains the modified records in order
	Records []Record `json:"Records"`

	// ShardId uniquely identifies the shard
	ShardId string `json:"shardId,omitempty"`

	// State holds optional tumbling window state [Tumbling Window]
	State json.RawMessage `json:"state,omitempty"`

	// Window holds the endpoints of this window [Tumbling Window]
	Window *Window `json:"window,omitempty"`
}

var reTableName = regexp.MustCompile(`\d{12}:table/([^/]+)/`)

// TableName extracts and returns the table name from a DynamoDB stream event source ARN.
// Returns the table name and true if successful, or empty string and false if the ARN format is invalid.
func TableName(eventSourceARN string) (string, bool) {
	match := reTableName.FindStringSubmatch(eventSourceARN)
	if len(match) != 2 {
		return "", false
	}
	return match[1], true
}
