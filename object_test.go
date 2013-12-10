package riaken_core

import (
	"testing"
)

import (
	"github.com/riaken/riaken-core/rpb"
)

func TestObject(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b1")
	object := bucket.Object("o1")
	if _, err := object.Store([]byte("o1-data")); err != nil {
		t.Error(err.Error())
	}
	data, err := object.Fetch()
	if err != nil {
		t.Error(err.Error())
	}
	if len(data.GetContent()) > 0 {
		if string(data.GetContent()[0].GetValue()) != "o1-data" {
			t.Errorf("got %s, expected o1-data", string(data.GetContent()[0].GetValue()))
		}
	}
	if ok, err := object.Delete(); !ok {
		t.Error("deletion of object failed")
	} else if err != nil {
		t.Error(err.Error())
	}
}

func TestObjectDo(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b1")
	object := bucket.Object("o1")
	tb1 := true
	opts1 := &rpb.RpbPutReq{
		ReturnBody: &tb1,
	}
	ret, err := object.Do(opts1).Store([]byte("o1-data"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(ret.GetContent()[0].GetValue()) != "o1-data" {
		t.Errorf("got %s, expected o1-data", string(ret.GetContent()[0].GetValue()))
	}

	tb2 := true
	opts2 := &rpb.RpbGetReq{
		Head: &tb2,
	}
	data, err := object.Do(opts2).Fetch()
	if err != nil {
		t.Error(err.Error())
	}
	if len(data.GetContent()) > 0 {
		if string(data.GetContent()[0].GetValue()) != "" {
			t.Error("expected empty content")
		}
	}

	rw := uint32(1)
	opts3 := &rpb.RpbDelReq{
		Rw: &rw,
	}
	if ok, err := object.Do(opts3).Delete(); !ok {
		t.Error("deletion of object failed")
	} else if err != nil {
		t.Error(err.Error())
	}
}

func TestObjectMultiple(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b1-multi")
	o1 := bucket.Object("o1m")
	o2 := bucket.Object("o2m")
	o3 := bucket.Object("o3m")

	if _, err := o1.Store([]byte("o1m-data")); err != nil {
		t.Error(err.Error())
	}
	if _, err := o2.Store([]byte("o1m-data")); err != nil {
		t.Error(err.Error())
	}
	if _, err := o3.Store([]byte("o1m-data")); err != nil {
		t.Error(err.Error())
	}

	var keys [][]byte
	for out, err := bucket.ListKeys(); !out.GetDone(); out, err = bucket.ListKeys() {
		if err != nil {
			t.Error(err.Error())
		}
		keys = append(keys, out.GetKeys()...)
	}

	if len(keys) != 3 {
		t.Errorf("expected: %s, got: %d", len(keys))
	}

	if _, err := o1.Delete(); err != nil {
		t.Error(err.Error())
	}
	if _, err := o2.Delete(); err != nil {
		t.Error(err.Error())
	}
	if _, err := o3.Delete(); err != nil {
		t.Error(err.Error())
	}
}

func TestObjectNoKey(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	bucket := session.GetBucket("b1")
	object := bucket.Object("")
	res, err := object.Store([]byte("o1-nokey"))
	if err != nil {
		t.Error(err.Error())
	}

	check := bucket.Object(string(res.GetKey()))
	data, err := check.Fetch()
	if err != nil {
		t.Error(err.Error())
	}
	if len(data.GetContent()) > 0 {
		if string(data.GetContent()[0].GetValue()) != "o1-nokey" {
			t.Errorf("got %s, expected o1-nokey", string(data.GetContent()[0].GetValue()))
		}
	}
	if ok, err := check.Delete(); !ok {
		t.Error("deletion of object failed")
	} else if err != nil {
		t.Error(err.Error())
	}
}
