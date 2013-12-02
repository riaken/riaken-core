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

func TestDoObject(t *testing.T) {
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
