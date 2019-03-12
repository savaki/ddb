package ddb

import (
	"bytes"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	reKeys   = regexp.MustCompile(`(#[a-zA-Z][a-zA-Z0-9._]*)`)
	reValues = regexp.MustCompile(`\?`)
)

type expression struct {
	attributes []*attributeSpec
	names      map[string]*string
	values     map[string]*dynamodb.AttributeValue
	index      int64

	adds    *bytes.Buffer
	deletes *bytes.Buffer
	removes *bytes.Buffer
	sets    *bytes.Buffer
}

func (e *expression) setExpressionAttributeName(name string) (string, error) {
	if e.names == nil {
		e.names = map[string]*string{}
	}

	for _, attr := range e.attributes {
		switch name[1:] {
		case attr.AttributeName:
			name = "#" + attr.FieldName
		case attr.FieldName:
			// ok
		default:
			continue
		}

		e.names[name] = aws.String(attr.AttributeName)
		return name, nil
	}

	return "", errorf(ErrInvalidFieldName, "invalid field name, %v", name)
}

func (e *expression) setExpressionAttributeValue(item *dynamodb.AttributeValue) string {
	if e.values == nil {
		e.values = map[string]*dynamodb.AttributeValue{}
	}

	id := atomic.AddInt64(&e.index, 1)
	name := ":field" + strconv.FormatInt(id, 10)
	e.values[name] = item

	return name
}

func (e *expression) String() string {
	size := e.Size()
	buf := bytes.NewBuffer(make([]byte, 0, size))

	//if e.adds != nil {
	//	buf.Write(e.adds.Bytes())
	//	io.WriteString(buf, " ")
	//}
	//if e.deletes != nil {
	//	buf.Write(e.deletes.Bytes())
	//	io.WriteString(buf, " ")
	//}
	//if e.removes != nil {
	//	buf.Write(e.removes.Bytes())
	//	io.WriteString(buf, " ")
	//}
	if e.sets != nil {
		buf.Write(e.sets.Bytes())
		io.WriteString(buf, " ")
	}

	// strip off trailing space
	if buf.Len() > 0 {
		buf.Truncate(buf.Len() - 1)
	}

	return buf.String()
}

func (e *expression) Size() int {
	size := 0
	//if e.adds != nil {
	//	size += e.adds.Len() + 1
	//}
	//if e.deletes != nil {
	//	size += e.deletes.Len() + 1
	//}
	//if e.removes != nil {
	//	size += e.removes.Len() + 1
	//}
	if e.sets != nil {
		size += e.sets.Len() + 1
	}
	return size
}

func (e *expression) append(buf *bytes.Buffer, keyword, expr string, values ...interface{}) error {
	var items []*dynamodb.AttributeValue
	for _, value := range values {
		item, err := marshal(value)
		if err != nil {
			return wrapf(err, ErrUnableToMarshalItem, "unable to marshal %v", reflect.TypeOf(value))
		}

		items = append(items, item)
	}

	// names
	//
	matches := reKeys.FindAllStringSubmatch(expr, -1)
	for _, match := range matches {
		name := match[1]
		updatedName, err := e.setExpressionAttributeName(name)
		if err != nil {
			return err
		}

		if name != updatedName {
			expr = strings.Replace(expr, name, updatedName, -1)
		}
	}

	// values
	//
	matches = reValues.FindAllStringSubmatch(expr, -1)
	if len(matches) != len(values) {
		return errorf(ErrMismatchedValueCount, "Set expression, %v, contains %v values, but received %v values", expr, len(matches), len(values))
	}
	for index := range matches {
		item := items[index]
		fieldName := e.setExpressionAttributeValue(item)
		expr = strings.Replace(expr, "?", fieldName, 1)
	}

	// expr
	//
	if buf.Len() == 0 {
		if len(keyword) > 0 {
			buf.WriteString(keyword)
			buf.WriteString(" ")
		}
	} else {
		buf.WriteString(", ")
	}
	buf.WriteString(strings.TrimSpace(expr))

	return nil
}

//func (e *expression) Add(expr string, values ...Value) error {
//	if e.adds == nil {
//		e.adds = bytes.NewBuffer(make([]byte, 0, 128))
//	}
//	return e.append(e.adds, "Add", expr, values...)
//}
//
//func (e *expression) Delete(expr string, values ...Value) error {
//	if e.deletes == nil {
//		e.deletes = bytes.NewBuffer(make([]byte, 0, 128))
//	}
//	return e.append(e.deletes, "Delete", expr, values...)
//}
//
//func (e *expression) Remove(expr string, values ...Value) error {
//	if e.removes == nil {
//		e.removes = bytes.NewBuffer(make([]byte, 0, 128))
//	}
//	return e.append(e.removes, "Remove", expr, values...)
//}

func (e *expression) Set(expr string, values ...interface{}) error {
	if e.sets == nil {
		e.sets = bytes.NewBuffer(make([]byte, 0, 128))
	}
	return e.append(e.sets, "Set", expr, values...)
}
