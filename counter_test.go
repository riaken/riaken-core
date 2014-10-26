package riaken_core

import (
	"testing"
)

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

func init() {
	// Set bucket properties.
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	opts := &rpb.RpbBucketProps{
		AllowMult: proto.Bool(true),
	}
	bucket := session.GetBucket("b5")
	if _, err := bucket.SetBucketProps(opts); err != nil {
		panic(err)
	}
}

func TestCounter(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b5")
	counter := bucket.Counter("c1")
	if _, err := counter.Update(1); err != nil {
		t.Error(err.Error())
	}
	data, err := counter.Get()
	if err != nil {
		t.Error(err.Error())
	}
	if data.GetValue() != 1 {
		t.Errorf("got %d, expected 1", data.GetValue())
	}

	if _, err := counter.Update(10); err != nil {
		t.Error(err.Error())
	}
	data, err = counter.Get()
	if err != nil {
		t.Error(err.Error())
	}
	if data.GetValue() != 11 {
		t.Errorf("got %d, expected 11", data.GetValue())
	}

	if _, err := counter.Update(-5); err != nil {
		t.Error(err.Error())
	}
	data, err = counter.Get()
	if err != nil {
		t.Error(err.Error())
	}
	if data.GetValue() != 6 {
		t.Errorf("got %d, expected 6", data.GetValue())
	}

	object := bucket.Object("c1")
	if ok, err := object.Delete(); !ok {
		t.Error("deletion of object failed")
	} else if err != nil {
		t.Error(err.Error())
	}
}

func TestCounterDo(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b5")
	counter := bucket.Counter("c1")
	opts1 := &rpb.RpbCounterUpdateReq{
		Returnvalue: proto.Bool(true),
	}
	ret, err := counter.Do(opts1).Update(5)
	if err != nil {
		t.Error(err.Error())
	}
	if ret.GetValue() != 5 {
		t.Errorf("got %d, expected 5", ret.GetValue())
	}

	opts2 := &rpb.RpbCounterGetReq{
		NotfoundOk: proto.Bool(true),
	}
	data, err := counter.Do(opts2).Get()
	if err != nil {
		t.Error(err.Error())
	}
	if data.GetValue() != 5 {
		t.Errorf("got %d, expected 5", ret.GetValue())
	}

	counter = bucket.Counter("c2")
	opts3 := &rpb.RpbCounterGetReq{
		NotfoundOk: proto.Bool(true),
	}
	data, err = counter.Do(opts3).Get()
	if err != nil {
		t.Error(err.Error())
	}
	if data.GetValue() != 0 {
		t.Errorf("got %d, expected 0", ret.GetValue())
	}

	object := bucket.Object("c1")
	if ok, err := object.Delete(); !ok {
		t.Error("deletion of object failed")
	} else if err != nil {
		t.Error(err.Error())
	}
}
