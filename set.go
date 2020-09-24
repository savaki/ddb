package ddb

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Int64Set represents an array expressed as a set.
// (otherwise than a List which would be the default)
type Int64Set []int64

// MarshalDynamoDBAttributeValue implements Marshaler
func (ii Int64Set) MarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	for _, i := range ii {
		item.NS = append(item.NS, aws.String(strconv.FormatInt(i, 10)))
	}
	return nil
}

// UnmarshalDynamoDBAttributeValue implements Unmarshaler
func (ii *Int64Set) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if item == nil || item.NS == nil {
		return nil
	}

	var vv []int64
	for _, ns := range item.NS {
		v, err := strconv.ParseInt(*ns, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse int64, %v: %w", *ns, err)
		}
		vv = append(vv, v)
	}

	*ii = vv
	return nil
}

// StringSet represents an array expressed as a set.
// (otherwise than a List which would be the default)
type StringSet []string

// MarshalDynamoDBAttributeValue implements Marshaler
func (ss StringSet) MarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if len(ss) > 0 && item != nil {
		item.SS = aws.StringSlice(ss)
	}
	return nil
}

// UnmarshalDynamoDBAttributeValue implements Unmarshaler
func (ss *StringSet) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if item == nil || item.SS == nil {
		return nil
	}

	vv := aws.StringValueSlice(item.SS)
	*ss = vv
	return nil
}
