package ddb

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Int64Set represents a DynamoDB Number Set (NS) type, storing a collection of int64 values.
// This is distinct from a List (L) type which would be the default for slices.
type Int64Set []int64

// MarshalDynamoDBAttributeValue marshals the Int64Set into a DynamoDB Number Set attribute.
// This implements the Marshaler interface for the AWS SDK v2.
//
//goland:noinspection ALL
func (ii Int64Set) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	var ns []string
	for _, i := range ii {
		ns = append(ns, strconv.FormatInt(i, 10))
	}
	return &types.AttributeValueMemberNS{Value: ns}, nil
}

// UnmarshalDynamoDBAttributeValue unmarshals a DynamoDB Number Set attribute into an Int64Set.
// This implements the Unmarshaler interface for the AWS SDK v2.
//
//goland:noinspection ALL
func (ii *Int64Set) UnmarshalDynamoDBAttributeValue(item types.AttributeValue) error {
	if item == nil {
		return nil
	}

	ns, ok := item.(*types.AttributeValueMemberNS)
	if !ok {
		return nil
	}

	var vv []int64
	for _, s := range ns.Value {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse int64, %v: %w", s, err)
		}
		vv = append(vv, v)
	}

	*ii = vv
	return nil
}

// StringSet represents a DynamoDB String Set (SS) type, storing a collection of string values.
// This is distinct from a List (L) type which would be the default for slices.
type StringSet []string

// Contains returns true if the specified string is found in the StringSet.
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

// ContainsRegexp returns true if the provided regular expression matches any element in the StringSet.
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

// StringSlice converts the StringSet to a standard []string slice.
//
//goland:noinspection ALL
func (ss StringSet) StringSlice() []string {
	return ss
}

// Sub returns a new StringSet containing elements from the original set
// that are not present in the provided set (set difference operation).
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

// MarshalDynamoDBAttributeValue marshals the StringSet into a DynamoDB String Set attribute.
// This implements the Marshaler interface for the AWS SDK v2.
//
//goland:noinspection ALL
func (ss StringSet) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	if len(ss) > 0 {
		return &types.AttributeValueMemberSS{Value: ss}, nil
	}
	return &types.AttributeValueMemberNULL{Value: true}, nil
}

// UnmarshalDynamoDBAttributeValue unmarshals a DynamoDB String Set attribute into a StringSet.
// This implements the Unmarshaler interface for the AWS SDK v2.
//
//goland:noinspection ALL
func (ss *StringSet) UnmarshalDynamoDBAttributeValue(item types.AttributeValue) error {
	if item == nil {
		return nil
	}

	s, ok := item.(*types.AttributeValueMemberSS)
	if !ok {
		return nil
	}

	*ss = s.Value
	return nil
}
