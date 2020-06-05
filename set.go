package ddb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// StringSet represents an array expressed as a set.
// (otherwise than a List which would be the default)
type StringSet []string

// MarshalDynamoDBAttributeValue implements Marshaler
func (s StringSet) MarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if len(s) > 0 && item != nil {
		item.SS = aws.StringSlice(s)
	}
	return nil
}

// UnmarshalDynamoDBAttributeValue implements Unmarshaler
func (s *StringSet) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if item == nil || item.SS == nil {
		return nil
	}

	ss := aws.StringValueSlice(item.SS)
	*s = ss
	return nil
}
