package line_protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type lineProtocolEncoder struct {
	buf    bytes.Buffer
	prev   bool
	hexBuf io.Writer
	Err    error
}

func NewEncoder() Encoder {
	return &lineProtocolEncoder{}
}

func (l *lineProtocolEncoder) Error() error {
	return l.Err
}

func (l *lineProtocolEncoder) Final() string {
	if l.Err != nil {
		panic(l.Err)
	}
	return l.buf.String()
}

func (l *lineProtocolEncoder) emitErrAtField(err error, key string) {
	if l.Err == nil {
		l.Err = errors.Wrapf(err, "at field %s", key)
	}
}

func (l *lineProtocolEncoder) checkState(k string) bool {
	if l.Err != nil {
		return false
	}
	if strings.Contains(k, "=") {
		l.Err = errors.Errorf("key %q contains one of char '='", k)
		return false
	}
	if len(k) > MaxKeyLength || len(k) == 0 {
		l.Err = errors.Errorf("key length %q does not met length range requirement", k)
		return false
	}
	return true
}

func (l *lineProtocolEncoder) writeHex(s []byte) {
	if l.hexBuf == nil {
		l.hexBuf = hex.NewEncoder(&l.buf)
	}
	l.hexBuf.Write(s)
}

func (l *lineProtocolEncoder) writeBase64(s []byte) {
	e := base64.NewEncoder(base64.URLEncoding, &l.buf)
	e.Write(s)
	e.Close()
}

func (l *lineProtocolEncoder) writeQuotedString(s string) {
	if strings.ContainsAny(s, "\";") {
		l.buf.WriteString("h\"")
		l.writeHex([]byte(s))
	} else {
		l.buf.WriteString("\"")
		l.buf.WriteString(s)
	}
	l.buf.WriteString("\"")
}

func (l *lineProtocolEncoder) writeQuotedBytes(s []byte) {
	l.buf.WriteString("b'")
	l.writeBase64(s)
	l.buf.WriteString("'")
}

func (l *lineProtocolEncoder) writeColon() {
	if l.prev {
		l.buf.WriteString(";")
	} else {
		l.prev = true
	}
}

func (l *lineProtocolEncoder) WriteJson(key string, s interface{}) error {
	if !l.checkState(key) {
		return l.Err
	}

	b, err := json.Marshal(s)
	if err != nil {
		l.emitErrAtField(err, key)
		return err
	}
	l.writeColon()
	l.buf.WriteString(key)
	l.buf.WriteString("=")
	l.writeQuotedBytes(b)
	return nil
}

func (l *lineProtocolEncoder) WriteProto(key string, s proto.Message) error {
	if !l.checkState(key) {
		return l.Err
	}

	b, err := protojson.Marshal(s)
	if err != nil {
		l.emitErrAtField(err, key)
		return err
	}
	l.writeColon()
	l.buf.WriteString(key)
	l.buf.WriteString("=")
	l.writeQuotedBytes(b)
	return nil
}

func (l *lineProtocolEncoder) WriteBool(key string, s bool) {
	if !l.checkState(key) {
		return
	}

	l.writeColon()
	l.buf.WriteString(key)
	l.buf.WriteString("=")
	if s {
		l.buf.WriteString("1")
	} else {
		l.buf.WriteString("0")
	}
}

func (l *lineProtocolEncoder) WriteString(key string, s string) {
	if !l.checkState(key) {
		return
	}

	l.writeColon()
	l.buf.WriteString(key)
	l.buf.WriteString("=")
	l.writeQuotedString(s)
}

func (l *lineProtocolEncoder) WriteBytes(key string, s []byte) {
	if !l.checkState(key) {
		return
	}

	l.writeColon()
	l.buf.WriteString(key)
	l.buf.WriteString("=")
	l.writeQuotedBytes(s)
}

func (l *lineProtocolEncoder) WriteInt64(key string, i int64) {
	if !l.checkState(key) {
		return
	}

	l.writeColon()
	l.buf.WriteString(key)
	if i < 0 {
		l.buf.WriteString("=-0x")
		l.buf.WriteString(strconv.FormatInt(-i, 16))
	} else {
		l.buf.WriteString("=0x")
		l.buf.WriteString(strconv.FormatInt(i, 16))
	}
}

func (l *lineProtocolEncoder) WriteUint64(key string, ui uint64) {
	if !l.checkState(key) {
		return
	}

	l.writeColon()
	l.buf.WriteString(key)
	l.buf.WriteString("=0x")
	l.buf.WriteString(strconv.FormatUint(ui, 16))
}

func (l *lineProtocolEncoder) WriteFloat64(key string, f float64, precision int) {
	if !l.checkState(key) {
		return
	}

	l.writeColon()
	l.buf.WriteString(key)
	if f < 0 {
		l.buf.WriteString("=-f")
		l.buf.WriteString(strconv.FormatFloat(-f, 'f', precision, 64))
	} else {
		l.buf.WriteString("=f")
		l.buf.WriteString(strconv.FormatFloat(f, 'f', precision, 64))
	}
}
