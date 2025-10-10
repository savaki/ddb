[![GoDoc](https://godoc.org/github.com/savaki/ddb?status.svg)](https://godoc.org/github.com/savaki/ddb)
[![Go Report Card](https://goreportcard.com/badge/github.com/savaki/ddb)](https://goreportcard.com/report/github.com/savaki/ddb)
[![Coverage Status](https://coveralls.io/repos/github/savaki/ddb/badge.svg)](https://coveralls.io/github/savaki/ddb)

ddb
--------------------------------------------------------

`ddb` is a high level library for accessing DynamoDB.

> **✅ You're on the v2 branch** - This is the current and recommended version!
>
> **Installation**: `go get github.com/savaki/ddb/v2`

## What's New in V2

🚀 **Performance** (20-60% faster)
- Fast-path marshaling for all Go primitive types (17 types - no reflection!)
- O(1) attribute name deduplication (was O(n) in v1)
- Pre-allocated maps reduce allocations by 10-45%
- Optimized expression assembly
- Real-world benchmarks: 20-60% faster for typical workloads

🔧 **AWS SDK v2**
- Modern AWS SDK with improved performance
- Context-aware APIs throughout
- Better error handling and type safety

✅ **Quality**
- Fixed race conditions
- Comprehensive benchmark suite (30+ benchmarks)
- All tests pass with race detector
- 100% API compatible with v1 (except SDK types)

## Upgrading from v1 to v2

Version 2.0.0 migrates from AWS SDK v1 to AWS SDK v2. This introduces some breaking changes:

### Breaking Changes

1. **Module Path**: Update your imports from `github.com/savaki/ddb` to `github.com/savaki/ddb/v2`

2. **DynamoDB Client**: The `New()` function now accepts the AWS SDK v2 client:
   ```go
   // v1
   import "github.com/aws/aws-sdk-go/service/dynamodb"
   api := dynamodb.New(session.Must(session.NewSession()))
   db := ddb.New(api)

   // v2
   import "github.com/aws/aws-sdk-go-v2/service/dynamodb"
   cfg, _ := config.LoadDefaultConfig(context.TODO())
   api := dynamodb.NewFromConfig(cfg)
   db := ddb.New(api)
   ```

3. **Billing Mode**: `WithBillingMode()` now accepts a typed constant instead of string:
   ```go
   // v1
   ddb.WithBillingMode(dynamodb.BillingModePayPerRequest)

   // v2
   import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
   ddb.WithBillingMode(types.BillingModePayPerRequest)
   ```

### What Hasn't Changed

- All table operations (Get, Put, Query, Scan, Update, Delete) work exactly the same
- Struct tags (`ddb` and `dynamodbav`) remain unchanged
- Transaction support is unchanged
- DynamoDB Streams support is unchanged

### QuickStart

```golang
type Example struct {
  PlayerID string `ddb:"hash"`
  Date     string `ddb:"range"`
}

func example() {
  var (
    ctx       = context.Background()
    s         = session.Must(session.NewSession(aws.NewConfig()))
    api       = dynamodb.New(s)
    tableName = "examples"
    model     = Example{}
    db        = ddb.New(api)
  )  
  
  table := db.MustTable(tableName, model)
  err := table.CreateTableIfNotExists(ctx)
  // handle err ...
  
  record := Example {
    PlayerID: "abc",
    Date:     "2019-01-01",
  }
  err = table.Put(record).RunWithContext(ctx) 
  // handle err ...
  
  var got Example
  err = table.Get(String(record.PlayerID)). // with hash key
    Range(String(record.Date)).             // and range key 
    ScanWithContext(ctx, &got)
  // handle err ...
  
  err = table.DeleteTableIfExists(ctx)
  // handle err ...
}
```

### Models

`ddb` leverages the `github.com/aws/aws-sdk-go-v2` package for encoding and decoding DynamoDB
records to and from structs.  

* Use `dynamodbav` tag option for encoding information
* Use `ddb` tag option to provide meta data about table
* Use `;` to separate multiple tag options within a tag e.g. `a;b;c`

#### Hash Key

Use the `hash` tag to define the hash (e.g. partition) key. 

```golang
type Example struct {
  ID string `ddb:"hash"`
}
```

#### Range Key

Use the `range` tag to define the range (e.g. sort) key. 

```golang
type Example struct {
  ID   string `ddb:"hash"`
  Date string `ddb:"range"`
}
```

#### Local Secondary Indexes (LSI)

To setup local secondary indexes, use the following tags:

* `lsi_range:{index-name}` define the range (e.g. sort) key of the LSI
* `lsi_range:{index-name},keys_only` - same as above, but indicate LSI should contains `KEYS_ONLY` 
* `lsi:{index-name}` include specific attribute within the LSI

In this example, we define a local secondary index with index name, `blah`, whose
range key is `Alt` that includes `Field1`.

```golang
type Example struct {
  ID     string `ddb:"hash"`
  Date   string `ddb:"range"`
  Alt    string `ddb:"lsi_range:blah"`
  Field1 string `ddb:"lsi:blah"`
  Field2 string
}
```

#### Global Secondary Indexes (GSI)

To setup global secondary indexes, use the following tags:

* `gsi_hash:{index-name}` define the hash (e.g. partition) key of the GSI
* `gsi_range:{index-name}` define the range (e.g. sort) key of the GSI
* `gsi_range:{index-name},keys_only` - same as above, but indicate GSI should contains `KEYS_ONLY` 
* `gsi:{index-name}` include specific attribute within the GSI

In this example, we define a global secondary index with index name, `blah`, whose
hash key is `VerifiedAt` and whose range key is `ID`.

```golang
type Example struct {
  ID         string `ddb:"hash;gsi_range:blah"`
  Date       string `ddb:"range"`
  VerifiedAt int64  `ddb:"gsi_hash:blah"`
}
```

#### Using `dynamodbav` to specify attribute values

This example illustrates using the `dynamodbav` in conjunction with the `ddb` to
define the schema.  Here the hash key of the table will be set to `id` (not `ID`)

```golang
type Example struct {
  ID string `ddb:"hash" dynamodbav:"id"`
}
```

### Testing

The `ddbtest` package provides utilities for testing with DynamoDB tables. The `WithTable` helper manages test resource lifecycle with automatic cleanup.

#### Using WithTable

`WithTable` takes a setup function and a test callback, ensuring cleanup occurs even if the test fails or panics:

```golang
import (
    "context"
    "testing"
    "github.com/savaki/ddb/v2"
    "github.com/savaki/ddb/v2/ddbtest"
)

type User struct {
    ID   string `ddb:"hash"`
    Name string
}

// Define a reusable setup function
func setupUserTable(t *testing.T) (context.Context, *ddb.Table, func(), error) {
    ctx := context.Background()
    cfg, _ := config.LoadDefaultConfig(ctx)
    api := dynamodb.NewFromConfig(cfg)
    db := ddb.New(api)

    tableName := "users-" + generateRandomID()
    table := db.MustTable(tableName, User{})

    err := table.CreateTableIfNotExists(ctx)
    if err != nil {
        return nil, nil, nil, err
    }

    cleanup := func() {
        table.DeleteTableIfExists(ctx)
    }

    return ctx, table, cleanup, nil
}

func TestUserOperations(t *testing.T) {
    ddbtest.WithTable(t, setupUserTable, func(t *testing.T, ctx context.Context, table *ddb.Table) {
        // Your test logic here
        user := User{ID: "123", Name: "Alice"}

        err := table.Put(user).RunWithContext(ctx)
        if err != nil {
            t.Fatalf("failed to put user: %v", err)
        }

        var got User
        err = table.Get(ddb.String("123")).ScanWithContext(ctx, &got)
        if err != nil {
            t.Fatalf("failed to get user: %v", err)
        }

        if got.Name != "Alice" {
            t.Errorf("expected name 'Alice', got '%s'", got.Name)
        }

        // Table cleanup happens automatically after this function returns
    })
}
```

The setup function returns:
- `ctx`: A context for the test
- `data`: Test-specific data (e.g., `*ddb.Table`, table name, etc.)
- `cleanup`: A function to clean up resources (called via `defer`)
- `err`: Any error that occurred during setup

This pattern ensures:
- Resources are properly cleaned up even if tests fail
- Setup code is reusable across multiple tests
- Test isolation through automatic teardown
