// Package ddbtest provides testing utilities for working with DynamoDB tables.
package ddbtest

import (
	"context"
	"testing"
)

// Callback is a test function that receives a testing context and data of type T.
// It's invoked by WithTable after successful setup to execute the actual test logic.
//
// Parameters:
//   - t: The testing.T instance for the current test
//   - ctx: A context.Context for the test, typically containing AWS configuration
//   - data: Test-specific data of type T (e.g., table name, ddb.Table instance, etc.)
type Callback[T any] func(t *testing.T, ctx context.Context, data T)

// Setup is a function that prepares test prerequisites and returns resources needed for testing.
// It follows the setup-teardown pattern common in integration tests.
//
// Returns:
//   - ctx: A context.Context for the test (may contain AWS configuration)
//   - data: Test-specific data of type T to pass to the test callback
//   - cleanup: A function to call for cleanup/teardown (invoked via defer)
//   - err: Any error that occurred during setup
//
// Example:
//
//	func setupDynamoDBTable(t *testing.T) (context.Context, *ddb.Table, func(), error) {
//	    ctx := context.Background()
//	    table := ddb.New(api, tableName, Model{})
//	    cleanup := func() {
//	        // Delete table resources
//	    }
//	    return ctx, table, cleanup, nil
//	}
type Setup[T any] func(t *testing.T) (ctx context.Context, data T, cleanup func())

// WithTable is a test helper that manages the lifecycle of test resources with automatic cleanup.
// It executes the setup function, runs the test callback with the provided data, and ensures
// cleanup occurs even if the test fails or panics.
//
// This pattern is particularly useful for integration tests that require DynamoDB table setup,
// ensuring tables are properly cleaned up after tests complete.
//
// Parameters:
//   - t: The testing.T instance for the current test
//   - setup: A Setup function that prepares test resources and returns cleanup logic
//   - callback: A Callback function containing the actual test logic to execute
//
// The function will call t.Fatalf if setup fails, preventing the test from running with
// invalid prerequisites.
//
// Example:
//
//	WithTable(t, setupDynamoDBTable, func(t *testing.T, ctx context.Context, table *ddb.Table) {
//	    // Test logic here using the prepared table
//	    item := Model{ID: "test-1", Status: "active"}
//	    err := table.Put(item).RunWithContext(ctx)
//	    assert.NoError(t, err)
//	})
func WithTable[T any](t *testing.T, setup Setup[T], callback Callback[T]) {
	ctx, data, cleanup := setup(t)
	defer cleanup()

	callback(t, ctx, data)
}
