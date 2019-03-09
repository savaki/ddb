// Copyright 2019 Matt Ho
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
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestDeleteTable(t *testing.T) {
	var (
		ctx       = context.Background()
		tableName = "blah"
	)

	t.Run("ok", func(t *testing.T) {
		mock := &Mock{}
		table := New(mock).MustTable(tableName, Example{})
		err := table.DeleteTableIfExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("table already exists", func(t *testing.T) {
		mock := &Mock{
			err: awserr.New(dynamodb.ErrCodeResourceNotFoundException, "boom", nil),
		}
		table := New(mock).MustTable(tableName, Example{})
		err := table.DeleteTableIfExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}
