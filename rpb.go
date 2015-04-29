package riaken_core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

var ErrZeroLength error = errors.New("response was only 0 bytes long")

// Codes are the inverse of Messages, generated in init() from Messages.
var Codes map[byte]string

// Messages are the human readable pb op codes.
var Messages = map[string]byte{
	"ErrorResp":         0,
	"PingReq":           1,
	"PingResp":          2,
	"GetClientIdReq":    3,
	"GetClientIdResp":   4,
	"SetClientIdReq":    5,
	"SetClientIdResp":   6,
	"GetServerInfoReq":  7,
	"GetServerInfoResp": 8,
	"GetReq":            9,
	"GetResp":           10,
	"PutReq":            11,
	"PutResp":           12,
	"DelReq":            13,
	"DelResp":           14,
	"ListBucketsReq":    15,
	"ListBucketsResp":   16,
	"ListKeysReq":       17,
	"ListKeysResp":      18,
	"GetBucketReq":      19,
	"GetBucketResp":     20,
	"SetBucketReq":      21,
	"SetBucketResp":     22,
	"MapRedReq":         23,
	"MapRedResp":        24,
	"IndexReq":          25,
	"IndexResp":         26,
	"SearchQueryReq":    27,
	"SearchQueryResp":   28,
	"ResetBucketReq":    29,
	"ResetBucketResp":   30,
	// Bucket Types
	"GetBucketTypeReq":   31,
	"SetBucketTypeReq":   32,
	"ResetBucketTypeReq": 33,
	// Riak CS
	"CSBucketReq":  40,
	"CSBucketResp": 41,
	// 1.4 Counters
	"CounterUpdateReq":  50,
	"CounterUpdateResp": 51,
	"CounterGetReq":     52,
	"CounterGetResp":    53,
	// Yokozuna
	"YokozunaIndexGetReq":    54,
	"YokozunaIndexGetResp":   55,
	"YokozunaIndexPutReq":    56,
	"YokozunaIndexDeleteReq": 57,
	"YokozunaSchemaGetReq":   58,
	"YokozunaSchemaGetResp":  59,
	"YokozunaSchemaPutReq":   60,
	// Riak 2 CRDT
	"DtFetchReq":   80,
	"DtFetchResp":  81,
	"DtUpdateReq":  82,
	"DtUpdateResp": 83,
	// Internal
	"AuthReq":  253,
	"AuthResp": 254,
	"StartTls": 255,
}

func init() {
	Codes = make(map[byte]string, len(Messages))
	for k, v := range Messages {
		Codes[v] = k
	}
}

// RpbRead reads the Riak response into the correct rpb structure.
func rpbRead(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, ErrZeroLength
	}

	code := data[0] // rpb code
	data = data[1:] // remaining data, if any
	switch code {
	case Messages["ErrorResp"]:
		out := &rpb.RpbErrorResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return nil, rpbRiakError(out)
	case Messages["PingResp"]:
		return true, nil
	case Messages["GetClientIdResp"]:
		out := &rpb.RpbGetClientIdResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["SetClientIdResp"]:
		return true, nil
	case Messages["GetServerInfoResp"]:
		out := &rpb.RpbGetServerInfoResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["GetResp"]:
		out := &rpb.RpbGetResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["PutResp"]:
		out := &rpb.RpbPutResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["DelResp"]:
		return true, nil
	case Messages["ListBucketsResp"]:
		out := &rpb.RpbListBucketsResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["ListKeysResp"]:
		out := &rpb.RpbListKeysResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["GetBucketResp"]:
		out := &rpb.RpbGetBucketResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["SetBucketResp"]:
		return true, nil
	case Messages["MapRedResp"]:
		out := &rpb.RpbMapRedResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["IndexResp"]:
		out := &rpb.RpbIndexResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["SearchQueryResp"]:
		out := &rpb.RpbSearchQueryResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["ResetBucketResp"]:
		return true, nil
	case Messages["SetBucketTypeResp"]:
		return true, nil
	case Messages["CSBucketResp"]:
		out := &rpb.RpbCSBucketResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["CounterUpdateResp"]:
		out := &rpb.RpbCounterUpdateResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["CounterGetResp"]:
		out := &rpb.RpbCounterGetResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["YokozunaIndexGetResp"]:
		out := &rpb.RpbYokozunaIndexGetResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["YokozunaIndexPutResp"]:
		return true, nil
	case Messages["YokozunaIndexSchemaGetResp"]:
		out := &rpb.RpbYokozunaSchemaGetResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["DtFetchResp"]:
		out := &rpb.DtFetchResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case Messages["DtUpdateResp"]:
		out := &rpb.DtUpdateResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	return nil, errors.New("invalid Rpb code specified")
}

// RpbWrite returns the correct data to write to a Riak rpb connection.
func rpbWrite(code byte, data []byte) ([]byte, error) {
	ml := new(bytes.Buffer)
	binary.Write(ml, binary.BigEndian, int32(len(data)+1)) // +1 for msg code
	mc := new(bytes.Buffer)
	binary.Write(mc, binary.BigEndian, int8(code))
	buf := []byte(ml.Bytes())
	buf = append(buf, mc.Bytes()...)
	buf = append(buf, data...)
	return buf, nil
}

// RpbRiakError converts a Riak RpbErrorResp into a Go error.
func rpbRiakError(err *rpb.RpbErrorResp) error {
	return errors.New(fmt.Sprintf("riak error [%d]: %s", err.GetErrcode(), err.GetErrmsg()))
}
