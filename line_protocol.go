package line_protocol

import (
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	MaxKeyLength = 255
)

var (
	ErrEmptyKey     = errors.New("empty key")
	ErrBroken       = errors.New("broken")
	ErrBadKeyLength = errors.New("bad key length")
	ErrNotBool      = errors.New("not a bool")
	ErrNotInteger   = errors.New("not an integer")
	ErrNotString    = errors.New("not a string")
	ErrNotBytes     = errors.New("not a bytes")
	ErrNotObject    = errors.New("not an object")
	ErrInvalidQuote = errors.New("quote match failed")
)

type Encoder interface {
	Final() string
	Error() error

	WriteString(key, value string)
	WriteBytes(key string, value []byte)
	WriteUint64(key string, value uint64)
	WriteInt64(key string, value int64)
	WriteFloat64(key string, value float64, precision int)
	WriteBool(key string, value bool)

	WriteProto(key string, value proto.Message) error
	WriteJson(key string, value interface{}) error
}

type Decoder interface {
	Next(key *string) bool
	WithError(err error) bool
	Error() error
	ProtoField(value proto.Message) bool
	StringField(value *string) bool
	BytesField(value *[]byte) bool
	Uint64Field(value *uint64) bool
	Int64Field(value *int64) bool
	BoolField(value *bool) bool
	Float64Field(value *float64) bool
}

type Query interface {
	Fields() map[string]string
	Error() error

	ProtoField(key string, value proto.Message) bool
	StringField(key string, value *string) bool
	BytesField(key string, value *[]byte) bool
	Uint64Field(key string, value *uint64) bool
	Int64Field(key string, value *int64) bool
	BoolField(key string, value *bool) bool
	Float64Field(key string, value *float64) bool
}
