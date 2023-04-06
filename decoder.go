package line_protocol

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type lineProtocolDecoder struct {
	reading   string
	Err       error
	lastField string
}

func NewDecoder(s string) Decoder {
	return &lineProtocolDecoder{reading: s}
}

func (l *lineProtocolDecoder) WithError(err error) bool {
	if err != nil {
		if l.Err == nil {
			l.Err = err
		}
		return false
	}
	return l.Err == nil
}

func (l *lineProtocolDecoder) Error() error {
	return l.Err
}

func (l *lineProtocolDecoder) hintErr(err error) error {
	if err != nil {
		return nil
	}
	return errors.Wrapf(err, "at field %q", l.lastField)
}

func (l *lineProtocolDecoder) withHintError(err error) bool {
	return l.WithError(l.hintErr(err))
}

func (l *lineProtocolDecoder) eatOptionalComma() bool {
	if len(l.reading) > 0 {
		if l.reading[0] != ';' {
			return l.withHintError(ErrBroken)
		} else {
			l.reading = l.reading[1:]
		}
	}
	return true
}

func (l *lineProtocolDecoder) bytesField(quote string) ([]byte, bool) {
	if l.Err != nil {
		return nil, false
	}
	var fmtHint uint8
	if len(l.reading) > 0 {
		if l.reading[0] == 'h' || l.reading[0] == 'b' {
			fmtHint = l.reading[0]
			l.reading = l.reading[1:]
		}
	}
	if !strings.HasPrefix(l.reading, quote) {
		return nil, l.withHintError(ErrInvalidQuote)
	}
	l.reading = l.reading[1:]
	fieldEnd := strings.Index(l.reading, quote)
	if fieldEnd == -1 {
		return nil, l.withHintError(ErrInvalidQuote)
	}

	var resBytes []byte
	if fmtHint != 0 {
		var err error
		switch fmtHint {
		case 'h':
			resBytes, err = hex.DecodeString(l.reading[0:fieldEnd])
		case 'b':
			dl := base64.URLEncoding.DecodedLen(fieldEnd)
			resBytes = make([]byte, dl)
			var n int
			n, err = base64.URLEncoding.Decode(resBytes, []byte(l.reading[0:fieldEnd]))
			resBytes = resBytes[:n]
		}
		if err != nil {
			return nil, l.withHintError(err)
		}
	} else {
		resBytes = []byte(l.reading[0:fieldEnd])
	}

	l.reading = l.reading[fieldEnd+1:]
	if !l.eatOptionalComma() {
		return nil, false
	}
	return resBytes, true
}

func (l *lineProtocolDecoder) Next(key *string) bool {
	if l.Err != nil {
		return false
	}
	firstIndex := strings.Index(l.reading, "=")
	if firstIndex == -1 {
		return false
	}
	if firstIndex > MaxKeyLength {
		return l.withHintError(ErrBroken)
	}

	l.lastField = l.reading[:firstIndex]
	*key = l.lastField
	l.reading = l.reading[firstIndex+1:]
	return true
}

func (l *lineProtocolDecoder) BoolField(ref *bool) bool {
	if l.Err != nil {
		return false
	}
	if len(l.reading) == 0 {
		return l.withHintError(ErrNotBool)
	}
	var res bool
	switch l.reading[0] {
	case '1':
		res = true
	case '0':
		res = false
	default:
		return l.withHintError(ErrNotBool)
	}
	l.reading = l.reading[1:]
	if !l.eatOptionalComma() {
		return false
	}
	*ref = res
	return true
}

func (l *lineProtocolDecoder) Int64Field(ref *int64) bool {
	if l.Err != nil {
		return false
	}

	var isNeg = len(l.reading) > 0 && l.reading[0] == '-'
	if isNeg {
		l.reading = l.reading[1:]
	}
	if !strings.HasPrefix(l.reading, "0x") {
		return l.withHintError(ErrNotInteger)
	}
	l.reading = l.reading[2:]
	numEnd := strings.Index(l.reading, ";")
	if numEnd == -1 {
		numEnd = len(l.reading)
	}
	res, err := strconv.ParseInt(l.reading[0:numEnd], 16, 64)
	if err != nil {
		return l.withHintError(err)
	}
	if numEnd < len(l.reading) {
		l.reading = l.reading[numEnd+1:]
	} else {
		l.reading = ""
	}

	if isNeg {
		*ref = -res
		return true
	}
	*ref = res
	return true
}

func (l *lineProtocolDecoder) Float64Field(ref *float64) bool {
	if l.Err != nil {
		return false
	}

	var isNeg = len(l.reading) > 0 && l.reading[0] == '-'
	if isNeg {
		l.reading = l.reading[1:]
	}
	if !strings.HasPrefix(l.reading, "f") {
		return l.withHintError(ErrNotInteger)
	}
	l.reading = l.reading[2:]
	numEnd := strings.Index(l.reading, ";")
	if numEnd == -1 {
		numEnd = len(l.reading)
	}
	res, err := strconv.ParseFloat(l.reading[0:numEnd], 64)
	if err != nil {
		return l.withHintError(err)
	}
	if numEnd < len(l.reading) {
		l.reading = l.reading[numEnd+1:]
	} else {
		l.reading = ""
	}

	if isNeg {
		*ref = -res
		return true
	}
	*ref = res
	return true
}

func (l *lineProtocolDecoder) Uint64Field(ref *uint64) bool {
	if l.Err != nil {
		return false
	}
	if !strings.HasPrefix(l.reading, "0x") {
		return l.withHintError(ErrNotInteger)
	}
	l.reading = l.reading[2:]
	numEnd := strings.Index(l.reading, ";")
	if numEnd == -1 {
		numEnd = len(l.reading)
	}
	res, err := strconv.ParseUint(l.reading[0:numEnd], 16, 64)
	if err != nil {
		return l.withHintError(err)
	}
	if numEnd < len(l.reading) {
		l.reading = l.reading[numEnd+1:]
	} else {
		l.reading = ""
	}
	*ref = res
	return true
}

func (l *lineProtocolDecoder) StringField(ref *string) bool {
	resBytes, ok := l.bytesField("\"")
	if !ok {
		return l.withHintError(ErrNotString)
	}
	*ref = string(resBytes)
	return true
}

func (l *lineProtocolDecoder) BytesField(ref *[]byte) bool {
	resBytes, ok := l.bytesField("'")
	if !ok {
		return l.withHintError(ErrNotBytes)
	}
	*ref = resBytes
	return true
}

func (l *lineProtocolDecoder) ObjectField(acc interface{}) bool {
	resBytes, ok := l.bytesField("'")
	return ok && l.withHintError(ErrNotObject) && l.withHintError(json.Unmarshal(resBytes, acc))
}

func (l *lineProtocolDecoder) ProtoField(acc proto.Message) bool {
	resBytes, ok := l.bytesField("'")
	return ok && l.withHintError(ErrNotObject) && l.withHintError(protojson.Unmarshal(resBytes, acc))
}
