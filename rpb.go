package riaken_core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

var ErrZeroLength error = errors.New("response was only 0 bytes long")

// RpbRead reads the Riak response into the correct rpb structure.
func rpbRead(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, ErrZeroLength
	}

	code := data[0] // rpb code
	data = data[1:] // remaining data, if any
	switch code {
	case 0: // RpbErrorResp
		out := &rpb.RpbErrorResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return nil, rpbRiakError(out)
	case 2: // RpbPingResp
		return true, nil
	case 4: // RpbGetClientIdResp
		out := &rpb.RpbGetClientIdResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 6: // RpbSetClientIdResp
		return true, nil
	case 8: // RpbGetServerInfoResp
		out := &rpb.RpbGetServerInfoResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 10: // RpbGetResp
		out := &rpb.RpbGetResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 12: // RpbPutResp
		out := &rpb.RpbPutResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 14: // RpbDelResp
		return true, nil
	case 16: // RpbListBucketsResp
		out := &rpb.RpbListBucketsResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 18: // RpbListKeysResp
		out := &rpb.RpbListKeysResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 20: // RpbGetBucketResp
		out := &rpb.RpbGetBucketResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 22: // RpbSetBucketResp
		return true, nil
	case 24: // RpbMapRedResp
		out := &rpb.RpbMapRedResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 26: // RpbIndexResp
		out := &rpb.RpbIndexResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 28: // RbpSearchQueryResp
		out := &rpb.RpbSearchQueryResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 51: // RpbCounterUpdateResp
		out := &rpb.RpbCounterUpdateResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	case 53: // RpbCounterGetResp
		out := &rpb.RpbCounterGetResp{}
		err := proto.Unmarshal(data, out)
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	return nil, errors.New("invalid Rpb code specified")
}

// RpbWrite returns the correct data to write to a Riak rpb connection.
func rpbWrite(code int, data []byte) ([]byte, error) {
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
