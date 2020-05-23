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
	"fmt"
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
	Message() string
}

type causer interface {
	Cause() error
}

type wrapper interface {
	Unwrap() error
}

func hasError(err error, code string) bool {
	if err == nil {
		return false
	}

	if v, ok := err.(Error); ok && v.Code() == code {
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

type baseError struct {
	code    string
	message string
	cause   error
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

func (b *baseError) Message() string {
	return b.message
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
