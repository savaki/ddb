# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`ddb` is a high-level Go library for accessing AWS DynamoDB. It provides a fluent API that wraps the AWS SDK, simplifying common DynamoDB operations while maintaining type safety through reflection and struct tags.

## Build and Test Commands

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run a specific test
go test -run TestName

# Build (validates compilation)
go build
```

## Core Architecture

### Table Specification System (spec.go)

The library uses reflection to analyze struct tags and build table specifications at runtime:

- `tableSpec`: Contains complete table metadata (hash/range keys, GSIs, LSIs, attributes)
- `indexSpec`: Defines secondary index structure
- `attributeSpec`: Maps struct fields to DynamoDB attributes
- The `inspect()` function parses `ddb` tags to construct these specifications

### Struct Tag System

Models use two tag systems:
- `ddb` tags: Define table schema (hash, range, gsi_hash, gsi_range, lsi_range, gsi, lsi)
- `dynamodbav` tags: Control AWS SDK marshaling (follows standard dynamodbattribute conventions)

Tags support multiple options separated by `;` and options within a tag separated by `,` (e.g., `ddb:"gsi_range:index_name,keys_only"`)

### Operation Builders (ddb.go and operation files)

Each DynamoDB operation (Get, Put, Query, Scan, Update, Delete) has a corresponding builder struct:

- Builders accumulate configuration through method chaining
- `RunWithContext(ctx)` or `Run()` executes the operation
- Operations that return data use `ScanWithContext(ctx, &target)` or `Scan(&target)` to unmarshal results
- All builders hold references to the DynamoDB API client and table spec

### Expression System (expression.go)

The library includes a custom expression parser that:
- Converts user-friendly expressions into DynamoDB expression syntax
- Uses `#` prefix for attribute names (e.g., `#FieldName`)
- Uses `?` placeholders for values that are replaced with actual values
- Automatically generates ExpressionAttributeNames and ExpressionAttributeValues maps
- Supports all expression types: Set, Remove, Add, Delete, Condition, Filter

Example: `"#Status = ?", "Status", "active"` becomes proper DynamoDB expression with attribute name substitution

### Transaction Support

The library provides transaction support through:
- `TransactWriteItemsWithContext()`: Executes multiple write operations atomically
- `TransactGetItemsWithContext()`: Retrieves multiple items atomically
- Automatic retry logic with exponential backoff for transaction conflicts
- `WriteTx` and `GetTx` interfaces allow operation builders to be used in transactions

Transactions automatically retry on conflict using exponential backoff (configurable via `WithTransactAttempts()` and `WithTransactTimeout()`).

### DynamoDB Streams Support (streams.go)

Provides types for processing DynamoDB Streams events in Lambda functions:
- `Event`: Top-level stream event structure
- `Record`: Individual change record with metadata
- `Change`: The actual DynamoDB item change (Keys, NewImage, OldImage)
- Compatible with `dynamodbattribute` unmarshaling (addresses limitations in aws-lambda-go)

## Key Design Patterns

1. **Fluent Interface**: All operations use method chaining for configuration
2. **Lazy Evaluation**: Builders accumulate state; nothing executes until Run/Scan methods
3. **Tag-Driven Schema**: Table structure derived from struct tags via reflection
4. **Capacity Tracking**: `ConsumedCapacity` accumulated across operations on a Table instance
5. **Error Accumulation**: Builders track errors internally; checked at execution time

## Testing Patterns

- Each file has a corresponding `_test.go` file
- Tests use the `mock_test.go` helper which provides mock DynamoDB API implementations
- The recent commit history shows a test builder pattern for constructing `ddb.Event` instances
