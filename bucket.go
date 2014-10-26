package riaken_core

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type Bucket struct {
	session     *Session // session reference
	name        string   // bucket name to associate with
	streamState int      // track state of streaming
	btype       []byte   // track the bucket type
}

func (b *Bucket) reset() {
	b.btype = []byte{}
}

// Type allows the bucket type to be set.  Chains with additional methods.
func (b *Bucket) Type(t []byte) *Bucket {
	b.btype = t
	return b
}

// ListKeys returns a list of keys for the associated bucket.
//
// This uses a streaming interface and should be called repeatedly until done is true.
//
//	var keys [][]byte
//	// Loop until done is received from Riak
//	for out, err := bucket.ListKeys(); !out.GetDone(); out, err = bucket.ListKeys() {
//		if err != nil {
//			t.Error(err.Error())
//			break
//		}
//		keys = append(keys, out.GetKeys()...)
//	}
//
// Riak docs - Not for production use: This operation requires traversing all keys stored in the cluster and should not be used in production.
func (b *Bucket) ListKeys() (*rpb.RpbListKeysResp, error) {
	var err error
	var out interface{}
	switch b.streamState {
	case 0:
		opts := &rpb.RpbListKeysReq{
			Bucket: []byte(b.name),
		}
		in, err := proto.Marshal(opts)
		if err != nil {
			return nil, err
		}
		out, err = b.session.execute(Messages["ListKeysReq"], in)
		if err != nil {
			return nil, err
		}
		b.streamState = 1

		// Fall through and do an initial read as well
	case 1:
		out, err = b.session.executeRead()
		if err != nil {
			return nil, err
		}
	}
	if out.(*rpb.RpbListKeysResp).GetDone() {
		b.streamState = 0
		b.reset() // reset only after done
	}
	return out.(*rpb.RpbListKeysResp), nil
}

// GetBucketProps returns the properties for this bucket.
func (b *Bucket) GetBucketProps() (*rpb.RpbGetBucketResp, error) {
	defer b.reset()
	opts := &rpb.RpbGetBucketReq{
		Bucket: []byte(b.name),
	}
	in, err := proto.Marshal(opts)
	out, err := b.session.execute(Messages["GetBucketReq"], in)
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbGetBucketResp), nil
}

// SetBucketProps set the properties for this bucket using RpbBucketProps.
func (b *Bucket) SetBucketProps(props *rpb.RpbBucketProps) (bool, error) {
	defer b.reset()
	opts := &rpb.RpbSetBucketReq{
		Bucket: []byte(b.name),
		Props:  props,
	}
	in, err := proto.Marshal(opts)
	if err != nil {
		return false, err
	}
	out, err := b.session.execute(Messages["SetBucketReq"], in)
	if err != nil {
		return false, err
	}
	return out.(bool), nil
}

// Object returns a new object associated with this bucket using key.
//
// Setting an empty key string will result in a server generated key.
func (b *Bucket) Object(key string) *Object {
	defer b.reset()
	return &Object{
		bucket: b,
		key:    key,
	}
}

// Counter returns a new counter associated with this bucket using key.
func (b *Bucket) Counter(key string) *Counter {
	defer b.reset()
	return &Counter{
		bucket: b,
		key:    key,
	}
}
