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
	"encoding/hex"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	ErrInvalidFieldName     = "InvalidFieldName"
	ErrItemNotFound         = "ItemNotFound"
	ErrMismatchedValueCount = "MismatchedValueCount"
	ErrUnableToMarshalItem  = "UnableToMarshalItem"
)

// Error provides a unified error definition that includes a code and message
// along with an optional original error.
type Error interface {
	error
	Cause() error
	Code() string
	Keys() (hashKey, rangeKey *dynamodb.AttributeValue)
	Message() string
	TableName() string
}

type causer interface {
	Cause() error
}

type coder interface {
	Code() string
}

type wrapper interface {
	Unwrap() error
}

func hasError(err error, code string) bool {
	if err == nil {
		return false
	}

	if v, ok := err.(coder); ok && v.Code() == code {
		return true
	}

	if item, ok := err.(causer); ok {
		return hasError(item.Cause(), code)
	}

	if item, ok := err.(wrapper); ok {
		return hasError(item.Unwrap(), code)
	}

	return false
}

// IsItemNotFoundError returns true if any error in the cause change contains the code, ErrItemNotFound
func IsItemNotFoundError(err error) bool {
	return hasError(err, ErrItemNotFound)
}

func IsMismatchedValueCountError(err error) bool {
	return hasError(err, ErrMismatchedValueCount)
}

func IsInvalidFieldNameError(err error) bool {
	return hasError(err, ErrInvalidFieldName)
}

func IsConditionalCheckFailedException(err error) bool {
	return hasError(err, dynamodb.ErrCodeConditionalCheckFailedException)
}

type baseError struct {
	code      string
	message   string
	cause     error
	hashKey   *dynamodb.AttributeValue
	rangeKey  *dynamodb.AttributeValue
	tableName string
}

func (b *baseError) Cause() error {
	return b.cause
}

func (b *baseError) Code() string {
	return b.code
}

func (b *baseError) Error() string {
	if b.cause == nil {
		return fmt.Sprintf("%v: %v", b.code, b.message)
	}
	return fmt.Sprintf("%v: %v: %v", b.code, b.message, b.cause.Error())
}

// Keys returns keys associated with error
// Not available for Transact* operations
func (b *baseError) Keys() (hashKey, rangeKey *dynamodb.AttributeValue) {
	return b.hashKey, b.rangeKey
}

func (b *baseError) Message() string {
	return b.message
}

func (b *baseError) TableName() string {
	return b.tableName
}

func (b *baseError) Unwrap() error {
	return b.cause
}

func errorf(code, message string, args ...interface{}) Error {
	return &baseError{
		code:    code,
		message: fmt.Sprintf(message, args...),
	}
}

// keyToString converts a dynamodb has or range key to string
func keyToString(key *dynamodb.AttributeValue) string {
	switch {
	case key == nil:
		return "null"
	case key.S != nil:
		return aws.StringValue(key.S)
	case key.N != nil:
		return aws.StringValue(key.N)
	case len(key.B) > 0:
		return hex.EncodeToString(key.B)
	default:
		return "null"
	}
}

// notFoundError generates a not found error for a given table
func notFoundError(hashKey, rangeKey *dynamodb.AttributeValue, tableName string) Error {
	var message string
	switch {
	case hashKey == nil && rangeKey == nil:
		message = "item not found"
	case rangeKey == nil:
		message = fmt.Sprintf("failed to find item, %v, in table, %v", keyToString(hashKey), tableName)
	default:
		message = fmt.Sprintf("failed to find item, %v#%v, in table, %v", keyToString(hashKey), keyToString(rangeKey), tableName)
	}

	return &baseError{
		code:      ErrItemNotFound,
		hashKey:   hashKey,
		message:   message,
		rangeKey:  rangeKey,
		tableName: tableName,
	}
}

func wrapf(cause error, code, message string, args ...interface{}) Error {
	if cause == nil {
		return nil
	}

	return &baseError{
		cause:   cause,
		code:    code,
		message: fmt.Sprintf(message, args...),
	}
}
