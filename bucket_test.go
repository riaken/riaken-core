package riaken_core

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

func TestBucketListKeys(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b2")
	object := bucket.Object("o2")
	if _, err := object.Store([]byte("o2-data")); err != nil {
		t.Error(err.Error())
	}

	var keys [][]byte
	// Loop until done is received from Riak
	for out, err := bucket.ListKeys(); !out.GetDone(); out, err = bucket.ListKeys() {
		if err != nil {
			t.Error(err.Error())
			break
		}
		keys = append(keys, out.GetKeys()...)
	}
	if len(keys) > 0 {
		if string(keys[0]) != "o2" {
			t.Errorf("expected: o2, got: %s", keys[0])
		}
	}

	if _, err := object.Delete(); err != nil {
		t.Error(err.Error())
	}
}

func TestBucketSetGetProps(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b2")
	props := &rpb.RpbBucketProps{
		NVal:      proto.Uint32(1),
		AllowMult: proto.Bool(true),
	}
	if ok, err := bucket.SetBucketProps(props); !ok {
		t.Error("could not set bucket props")
	} else if err != nil {
		t.Error(err.Error())
	}

	out, err := bucket.GetBucketProps()
	if err != nil {
		t.Error(err.Error())
	}
	if out.GetProps().GetAllowMult() != true {
		t.Errorf("expected: true, got: %t", out.GetProps().GetAllowMult())
	}
}

func TestBucketSetBucketType(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b2").Type("test_maps")
	props := &rpb.RpbBucketProps{
		AllowMult: proto.Bool(true),
	}
	if ok, err := bucket.SetBucketType(props); !ok {
		t.Error("could not set bucket props")
	} else if err != nil {
		t.Error(err.Error())
	}
}

func TestBucketResetBucket(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b2").Type("test_maps")
	if ok, err := bucket.ResetBucket(); !ok {
		t.Error("could not set bucket props")
	} else if err != nil {
		t.Error(err.Error())
	}
}
