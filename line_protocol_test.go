package line_protocol

import (
	"math"
	"testing"
)

type EncodeMagicType = uint8

const (
	EncodeMagicString = iota
	EncodeMagicUint
	EncodeMagicInt
	EncodeMagicBool
	EncodeMagicBytes
	EncodeMagicFloat
	EncodeMax
	//EncodeMagicJson
)

func fuzzEncode(l Encoder, key string, magic EncodeMagicType, i int64, s string) {
	switch magic {
	case EncodeMagicString:
		l.WriteString(key, s)
	case EncodeMagicUint:
		l.WriteUint64(key, uint64(i))
	case EncodeMagicInt:
		l.WriteInt64(key, i)
	case EncodeMagicFloat:
		l.WriteFloat64(key, float64(i)/1000, 3)
	case EncodeMagicBool:
		l.WriteBool(key, (i&1) == 1)
	case EncodeMagicBytes:
		l.WriteBytes(key, []byte(s))
	}
}

func checkField(t *testing.T, key string, magic EncodeMagicType, i int64, s string, d Decoder) {
	switch magic {
	case EncodeMagicString:
		var v string
		if !d.StringField(&v) {
			t.Fatalf("expected string field %q", key)
		}
		if v != s {
			t.Fatalf("expected string field %q to be %q, got %q", key, s, v)
		}
	case EncodeMagicUint:
		var v uint64
		if !d.Uint64Field(&v) {
			t.Fatalf("expected uint field %q", key)
		}
		if v != uint64(i) {
			t.Fatalf("expected uint field %q to be %d, got %d", key, i, v)
		}
	case EncodeMagicInt:
		var v int64
		if !d.Int64Field(&v) {
			t.Fatalf("expected int field %q", key)
		}
		if v != i {
			t.Fatalf("expected int field %q to be %d, got %d", key, i, v)
		}
	case EncodeMagicFloat:
		var v float64
		if !d.Float64Field(&v) {
			t.Fatalf("expected float field %q", key)
		}
		if math.Abs(v-float64(i)/1000) > 0.0001 {
			t.Fatalf("expected float field %q to be %f, got %f", key, float64(i)/1000, v)
		}
	case EncodeMagicBool:
		var v bool
		if !d.BoolField(&v) {
			t.Fatalf("expected bool field %q", key)
		}
		if v != ((i & 1) == 1) {
			t.Fatalf("expected bool field %q to be %t, got %t", key, (i&1) == 1, v)
		}
	case EncodeMagicBytes:
		var v []byte
		if !d.BytesField(&v) {
			t.Fatalf("expected bytes field %q", key)
		}
		if string(v) != s {
			t.Fatalf("expected bytes field %q to be %q, got %q", key, s, v)
		}
	default:
		t.Fatalf("unknown magic type %d", magic)
	}
}

func checkQueryField(t *testing.T, q Query, key string, magic EncodeMagicType, i int64, s string) {
	switch magic {
	case EncodeMagicString:
		var v string
		if !q.StringField(key, &v) {
			t.Fatalf("expected string field %q", key)
		}
		if v != s {
			t.Fatalf("expected string field %q to be %q, got %q", key, s, v)
		}
	case EncodeMagicUint:
		var v uint64
		if !q.Uint64Field(key, &v) {
			t.Fatalf("expected uint field %q", key)
		}
		if v != uint64(i) {
			t.Fatalf("expected uint field %q to be %d, got %d", key, i, v)
		}
	case EncodeMagicInt:
		var v int64
		if !q.Int64Field(key, &v) {
			t.Fatalf("expected int field %q", key)
		}
		if v != i {
			t.Fatalf("expected int field %q to be %d, got %d", key, i, v)
		}
	case EncodeMagicFloat:
		var v float64
		if !q.Float64Field(key, &v) {
			t.Fatalf("expected float field %q", key)
		}
		if math.Abs(v-float64(i)/1000) > 0.0001 {
			t.Fatalf("expected float field %q to be %f, got %f", key, float64(i)/1000, v)
		}
	case EncodeMagicBool:
		var v bool
		if !q.BoolField(key, &v) {
			t.Fatalf("expected bool field %q", key)
		}
		if v != ((i & 1) == 1) {
			t.Fatalf("expected bool field %q to be %t, got %t", key, (i&1) == 1, v)
		}
	case EncodeMagicBytes:
		var v []byte
		if !q.BytesField(key, &v) {
			t.Fatalf("expected bytes field %q", key)
		}
		if string(v) != s {
			t.Fatalf("expected bytes field %q to be %q, got %q", key, s, v)
		}
	default:
		t.Fatalf("unknown magic type %d", magic)
	}
}

func FuzzLineProtocol(f *testing.F) {
	f.Fuzz(func(t *testing.T,
		nn uint8,
		k1 string, t1 uint8, i1 int64, s1 string,
		k2 string, t2 uint8, i2 int64, s2 string,
		k3 string, t3 uint8, i3 int64, s3 string,
		k4 string, t4 uint8, i4 int64, s4 string,
		k5 string, t5 uint8, i5 int64, s5 string) {
		if nn > 5 || k1 == k2 || k2 == k3 || k3 == k4 || k4 == k5 || k1 == k3 || k1 == k4 || k1 == k5 || k2 == k4 || k2 == k5 || k3 == k5 {
			return
		}
		if t1 >= EncodeMax || t2 >= EncodeMax || t3 >= EncodeMax || t4 >= EncodeMax || t5 >= EncodeMax {
			return
		}
		var enc = NewEncoder()
		if nn >= 1 {
			fuzzEncode(enc, k1, t1, i1, s1)
		}
		if nn >= 2 {
			fuzzEncode(enc, k2, t2, i2, s2)
		}
		if nn >= 3 {
			fuzzEncode(enc, k3, t3, i3, s3)
		}
		if nn >= 4 {
			fuzzEncode(enc, k4, t4, i4, s4)
		}
		if nn >= 5 {
			fuzzEncode(enc, k5, t5, i5, s5)
		}

		if enc.Error() != nil {
			return
		}

		var s = enc.Final()
		var dec = NewDecoder(s)
		var fieldName string
		var expNN uint8 = 0
		for dec.Next(&fieldName) {
			switch fieldName {
			case k1:
				if expNN != 0 {
					t.Fatalf("expected to be first field")
				}
				expNN++
				checkField(t, fieldName, t1, i1, s1, dec)
			case k2:
				if expNN != 1 {
					t.Fatalf("expected to be second field")
				}
				expNN++
				checkField(t, fieldName, t2, i2, s2, dec)
			case k3:
				if expNN != 2 {
					t.Fatalf("expected to be third field")
				}
				expNN++
				checkField(t, fieldName, t3, i3, s3, dec)
			case k4:
				if expNN != 3 {
					t.Fatalf("expected to be fourth field")
				}
				expNN++
				checkField(t, fieldName, t4, i4, s4, dec)
			case k5:
				if expNN != 4 {
					t.Fatalf("expected to be fifth field")
				}
				expNN++
				checkField(t, fieldName, t5, i5, s5, dec)
			default:
				t.Fatalf("unexpected field %q", fieldName)
			}
		}
		if expNN != nn {
			t.Fatalf("expected %d fields, got %d", nn, expNN)
		}

		var query = NewQuery(s)
		if query.Error() != nil {
			t.Fatalf("unexpected error: %s", query.Error())
		}
		if len(query.Fields()) != int(nn) {
			t.Fatalf("expected %d fields, got %d", nn, len(query.Fields()))
		}
		if nn >= 1 {
			checkQueryField(t, query, k1, t1, i1, s1)
		}
		if nn >= 2 {
			checkQueryField(t, query, k2, t2, i2, s2)
		}
		if nn >= 3 {
			checkQueryField(t, query, k3, t3, i3, s3)
		}
		if nn >= 4 {
			checkQueryField(t, query, k4, t4, i4, s4)
		}
		if nn >= 5 {
			checkQueryField(t, query, k5, t5, i5, s5)
		}
	})
}
