# ddb
--------------------------------------------------------

`ddb` is a high level library for accessing DynamoDB.

### QuickStart

```golang
type Example struct {
  PlayerID string `ddb:"hash_key"`
  Date     string `ddb:"range_key"`
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
  
  err = table.DeleteTableIfExists(ctx)
  // handle err ...
}
```

### Models

`ddb` leverages the the `github.com/aws/aws-sdk-go` package for encoding and decoding DynamoDB
records to and from structs.  

* Use `dynamodbav` tag option for encoding information
* Use `ddb` tag option to provide meta data about table
* Use `;` to separate multiple tag options within a tag e.g. `a;b;c`

#### Hash Key

Use the `hash_key` tag to define the hash (e.g. partition) key. 

```golang
type Table struct {
  ID string `ddb:"hash_key"`
}
```

#### Range Key

Use the `range_key` tag to define the range (e.g. sort) key. 

```golang
type Table struct {
  ID   string `ddb:"hash_key"`
  Date string `ddb:"range_key"`
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
type Table struct {
  ID     string `ddb:"hash_key"`
  Date   string `ddb:"range_key"`
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
type Table struct {
  ID         string `ddb:"hash_key;gsi_range:blah"`
  Date       string `ddb:"range_key"`
  VerifiedAt int64  `ddb:"gsi_hash:blah"`
}
```
