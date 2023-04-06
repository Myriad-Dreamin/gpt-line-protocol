package line_protocol

import (
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type lineProtocolQuery struct {
	d      lineProtocolDecoder
	fields map[string]string
}

func NewQuery(s string) Query {
	var l = &lineProtocolQuery{}
	l.Load(s)
	return l
}

func (l *lineProtocolQuery) Error() error {
	return l.d.Err
}

func (l *lineProtocolQuery) Fields() map[string]string {
	return l.fields
}

func (l *lineProtocolQuery) hintErr(err error, pos int) {
	l.d.Err = errors.Wrapf(err, "at offset %d", pos)
}

func (l *lineProtocolQuery) Load(s string) bool {
	var off = 0
	l.fields = make(map[string]string)
	l.d.Err = nil
	for off < len(s) {
		var key, value string
		if i := strings.IndexByte(s[off:], '='); i >= 0 {
			key = s[off : off+i]
			off += i + 1
		} else {
			l.hintErr(ErrBroken, off)
			break
		}

		if len(key) == 0 || len(key) > MaxKeyLength {
			l.hintErr(ErrBadKeyLength, off)
			break
		}

		if i := strings.IndexByte(s[off:], ';'); i >= 0 {
			value = s[off : off+i]
			off += i + 1
		} else {
			value = s[off:]
			off = len(s)
		}

		l.fields[key] = value
	}

	return l.d.Err == nil
}

func (l *lineProtocolQuery) RawField(k string, v *string) bool {
	if l.d.Err != nil {
		return false
	}
	if s, ok := l.fields[k]; ok {
		*v = s
		return true
	}
	return false
}

func (l *lineProtocolQuery) locate(k string) bool {
	var rv string
	if !l.RawField(k, &rv) {
		return false
	}
	l.d.lastField = k
	l.d.reading = rv
	return true
}

func (l *lineProtocolQuery) terminated() bool {
	if len(l.d.reading) > 0 {
		l.d.hintErr(ErrBroken)
		return false
	}
	return true
}

func (l *lineProtocolQuery) Int64Field(k string, v *int64) bool {
	return l.locate(k) && l.d.Int64Field(v) && l.terminated()
}

func (l *lineProtocolQuery) Uint64Field(k string, v *uint64) bool {
	return l.locate(k) && l.d.Uint64Field(v) && l.terminated()
}

func (l *lineProtocolQuery) StringField(k string, v *string) bool {
	return l.locate(k) && l.d.StringField(v) && l.terminated()
}

func (l *lineProtocolQuery) BytesField(k string, v *[]byte) bool {
	return l.locate(k) && l.d.BytesField(v) && l.terminated()
}

func (l *lineProtocolQuery) ObjectField(k string, v interface{}) bool {
	return l.locate(k) && l.d.ObjectField(v) && l.terminated()
}

func (l *lineProtocolQuery) ProtoField(k string, v proto.Message) bool {
	return l.locate(k) && l.d.ProtoField(v) && l.terminated()
}

func (l *lineProtocolQuery) BoolField(k string, v *bool) bool {
	return l.locate(k) && l.d.BoolField(v) && l.terminated()
}

func (l *lineProtocolQuery) Float64Field(k string, v *float64) bool {
	return l.locate(k) && l.d.Float64Field(v) && l.terminated()
}
