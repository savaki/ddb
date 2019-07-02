package ddb

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type ScanTable struct {
	ID string `dynamodbav:"id" ddb:"hash"`
}

func TestScan_First(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			want  = ScanTable{ID: "abc"}
			mock  = &Mock{scanItems: []interface{}{want}}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got ScanTable
		err := table.Scan().First(&got)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("not found", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got ScanTable
		err := table.Scan().First(&got)
		if !IsItemNotFoundError(err) {
			t.Fatalf("got %#v; want ErrItemNotFound", err)
		}
	})

	t.Run("aws err", func(t *testing.T) {
		var (
			want  = io.EOF
			mock  = &Mock{err: io.EOF}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got ScanTable
		err := table.Scan().First(&got)
		if err == nil {
			t.Fatalf("got %v; want %v", err, want)
		}
	})
}

func TestScan_Each(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			item1 = ScanTable{ID: "abc"}
			item2 = ScanTable{ID: "def"}
			want  = []ScanTable{item1, item2}
			mock  = &Mock{scanItems: []interface{}{item1, item2}}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got []ScanTable
		err := table.Scan().Each(func(item Item) (bool, error) {
			var v ScanTable
			if err := item.Unmarshal(&v); err != nil {
				return false, nil
			}
			got = append(got, v)
			return true, nil
		})

		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, item1)
		}
	})

	t.Run("stop early", func(t *testing.T) {
		var (
			item1 = ScanTable{ID: "abc"}
			item2 = ScanTable{ID: "def"}
			want  = []ScanTable{item1}
			mock  = &Mock{scanItems: []interface{}{item1, item2}}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		var got []ScanTable
		err := table.Scan().Each(func(item Item) (bool, error) {
			var v ScanTable
			if err := item.Unmarshal(&v); err != nil {
				return false, nil
			}
			got = append(got, v)
			return false, nil
		})

		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, item1)
		}
	})
}

func TestScan_Condition(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var (
			mock  = &Mock{}
			db    = New(mock)
			table = db.MustTable("example", ScanTable{})
		)

		input := table.Scan().
			Filter("#ID = ?", "abc").
			makeScanInput(0, 1, nil)

		assertEqual(t, input, "testdata/scan_condition.json")
	})
}

func TestScan_ConditionLive(t *testing.T) {
	if !runIntegrationTests {
		t.SkipNow()
	}

	type Sample struct {
		ID string `ddb:"hash"`
	}

	var (
		ctx  = context.Background()
		s, _ = session.NewSession(aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
			WithRegion("us-west-2").
			WithEndpoint("http://localhost:8000"))
		api       = dynamodb.New(s)
		tableName = fmt.Sprintf("scan-%v", time.Now().UnixNano())
		table     = New(api).MustTable(tableName, Sample{})
	)

	err := table.CreateTableIfNotExists(ctx)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	defer table.DeleteTableIfExists(ctx)

	err = table.Put(Sample{ID: "a"}).RunWithContext(ctx)
	assert.Nil(t, err)

	err = table.Put(Sample{ID: "b"}).RunWithContext(ctx)
	assert.Nil(t, err)

	err = table.Put(Sample{ID: "c"}).RunWithContext(ctx)
	assert.Nil(t, err)

	var samples []Sample
	fn := func(item Item) (bool, error) {
		var sample Sample
		if err := item.Unmarshal(&sample); err != nil {
			return false, err
		}
		samples = append(samples, sample)
		return true, nil
	}

	err = table.Scan().
		ConsistentRead(true).
		Filter("#ID = ?", "b").
		TotalSegments(3).
		EachWithContext(ctx, fn)
	assert.Nil(t, err)
	assert.Len(t, samples, 1)
	assert.Equal(t, Sample{ID: "b"}, samples[0])
}

func TestScan_ConsistentRead(t *testing.T) {
	s := &Scan{
		expr: &expression{},
		spec: &tableSpec{TableName: "example"},
	}
	s.ConsistentRead(true)
	input := s.makeScanInput(1, 2, nil)

	if input.ConsistentRead == nil {
		t.Fatalf("got nil; want not nil")
	}
	if !*input.ConsistentRead {
		t.Fatalf("got false; want true")
	}
}
