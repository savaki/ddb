package ddb

import (
	"bytes"
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	reKeys   = regexp.MustCompile(`(#[a-zA-Z][a-zA-Z0-9._]*)`)
	reValues = regexp.MustCompile(`\?`)
)

type Update struct {
	api            dynamodbiface.DynamoDBAPI
	spec           *tableSpec
	hashKey        Value
	rangeKey       Value
	consistentRead bool
	names          map[string]*string
	values         map[string]*dynamodb.AttributeValue
	consumed       *ConsumedCapacity
	index          int64
	err            error
	set            *bytes.Buffer
}

func (u *Update) makeUpdateExpression() string {
	n := 0
	if u.set != nil {
		n += u.set.Len()
	}

	buf := bytes.NewBuffer(make([]byte, 0, n))
	if u.set.Len() > 0 {
		buf.WriteString(u.set.String())
	}

	return buf.String()
}

func (u *Update) makeUpdateItemInput() *dynamodb.UpdateItemInput {
	var (
		key              = makeKey(u.spec, u.hashKey, u.rangeKey)
		updateExpression = u.makeUpdateExpression()
	)

	return &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  u.names,
		ExpressionAttributeValues: u.values,
		Key:                       key,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(u.spec.TableName),
		UpdateExpression:          aws.String(updateExpression),
	}
}

func (u *Update) Range(value Value) *Update {
	u.rangeKey = value
	return u
}

func (u *Update) setExpressionAttributeName(name string) string {
	if u.names == nil {
		u.names = map[string]*string{}
	}

	for _, attr := range u.spec.Attributes {
		switch name[1:] {
		case attr.AttributeName:
			name = "#" + attr.FieldName
		case attr.FieldName:
			// ok
		default:
			continue
		}

		u.names[name] = aws.String(attr.AttributeName)
		return name
	}

	u.err = errorf(ErrInvalidFieldName, "invalid field name, %v", name)
	return ""
}

func (u *Update) setExpressionAttributeValue(value Value) string {
	if u.values == nil {
		u.values = map[string]*dynamodb.AttributeValue{}
	}

	id := atomic.AddInt64(&u.index, 1)
	name := ":field" + strconv.FormatInt(id, 10)
	u.values[name] = value.item

	return name
}

func (u *Update) Set(expr string, values ...Value) *Update {
	expr = strings.TrimSpace(expr)

	// handle keys
	matches := reKeys.FindAllStringSubmatch(expr, -1)
	for _, match := range matches {
		name := match[1]
		updatedName := u.setExpressionAttributeName(name)
		if name != updatedName {
			expr = strings.Replace(expr, name, updatedName, -1)
		}
	}

	// handle values
	matches = reValues.FindAllStringSubmatch(expr, -1)
	if len(matches) != len(values) {
		u.err = errorf(ErrMismatchedValueCount, "Set expression, %v, contains %v values, but received %v values", expr, len(matches), len(values))
		return u
	}
	for index := range matches {
		value := values[index]
		fieldName := u.setExpressionAttributeValue(value)
		expr = strings.Replace(expr, "?", fieldName, 1)
	}

	if u.set == nil {
		u.set = bytes.NewBuffer(nil)
	}

	if u.set.Len() == 0 {
		u.set.WriteString("Set ")
	} else {
		u.set.WriteString(", ")
	}
	u.set.WriteString(expr)

	return u
}

func (u *Update) RunWithContext(ctx context.Context) error {
	if u.err != nil {
		return u.err
	}

	input := u.makeUpdateItemInput()
	output, err := u.api.UpdateItemWithContext(ctx, input)
	if err != nil {
		return err
	}

	u.consumed.add(output.ConsumedCapacity)
	return nil
}

func (u *Update) Run() error {
	return u.RunWithContext(defaultContext)
}

func (t *Table) Update(hashKey Value) *Update {
	return &Update{
		api:      t.ddb.api,
		spec:     t.spec,
		hashKey:  hashKey,
		consumed: t.consumed,
	}
}
