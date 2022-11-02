package ddb

import (
	"fmt"
	"regexp"
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

// Contains returns true if want is contained in the StringSet
func (ss StringSet) Contains(want string) bool {
	for _, s := range ss {
		if want == s {
			return true
		}
	}
	return false
}

// ContainsRegexp returns true if re matches any element of the Regexp
func (ss StringSet) ContainsRegexp(re *regexp.Regexp) bool {
	for _, s := range ss {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// StringSlice returns StringSet as []string
func (ss StringSet) StringSlice() []string {
	return ss
}

// Sub returns a new StringSet that contains the original StringSet minus
// the elements contained in the provided StringSet
func (ss StringSet) Sub(that StringSet) StringSet {
	var results StringSet

loop:
	for _, s := range ss {
		for _, t := range that {
			if s == t {
				continue loop
			}
		}
		results = append(results, s)
	}

	return results
}

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
