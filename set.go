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

// Contains returns true if want is contained in the StringSet
//
//goland:noinspection ALL
func (ii Int64Set) Contains(want int64) bool {
	for _, i := range ii {
		if want == i {
			return true
		}
	}
	return false
}

// Sub returns a new StringSet that contains the original StringSet minus
// the elements contained in the provided StringSet
//
//goland:noinspection ALL
func (ii Int64Set) Sub(that Int64Set) Int64Set {
	var results Int64Set

loop:
	for _, i := range ii {
		for _, t := range that {
			if i == t {
				continue loop
			}
		}
		results = append(results, i)
	}

	return results
}

// MarshalDynamoDBAttributeValue implements Marshaler
//
//goland:noinspection ALL
func (ii Int64Set) MarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	for _, i := range ii {
		item.NS = append(item.NS, aws.String(strconv.FormatInt(i, 10)))
	}
	return nil
}

// UnmarshalDynamoDBAttributeValue implements Unmarshaler
//
//goland:noinspection ALL
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
//
//goland:noinspection ALL
func (ss StringSet) Contains(want string) bool {
	for _, s := range ss {
		if want == s {
			return true
		}
	}
	return false
}

// ContainsRegexp returns true if regexp matches any element of the Regexp
//
//goland:noinspection ALL
func (ss StringSet) ContainsRegexp(re *regexp.Regexp) bool {
	for _, s := range ss {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// StringSlice returns StringSet as []string
//
//goland:noinspection ALL
func (ss StringSet) StringSlice() []string {
	return ss
}

// Sub returns a new StringSet that contains the original StringSet minus
// the elements contained in the provided StringSet
//
//goland:noinspection ALL
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
//
//goland:noinspection ALL
func (ss StringSet) MarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if len(ss) > 0 && item != nil {
		item.SS = aws.StringSlice(ss)
	}
	return nil
}

// UnmarshalDynamoDBAttributeValue implements Unmarshaler
//
//goland:noinspection ALL
func (ss *StringSet) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if item == nil || item.SS == nil {
		return nil
	}

	vv := aws.StringValueSlice(item.SS)
	*ss = vv
	return nil
}
