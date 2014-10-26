package riaken_core

import (
	"errors"

	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type Object struct {
	bucket *Bucket // bucket this object is associated with
	key    string  // key this object is associated with
	vclock []byte  // vector clock
	opts   interface{}
	ct     []byte
}

func (o *Object) reset() {
	o.opts = nil
	o.ct = nil
}

func (o *Object) GetOpts() interface{} {
	return o.opts
}

// Do allows opts to be passed to a method.  This call should be chained.
func (o *Object) Do(opts interface{}) *Object {
	o.opts = opts
	return o
}

// ContentType sets the default content type for this object.
func (o *Object) ContentType(ct []byte) {
	o.ct = ct
}

// Fetch returns the data for this object at key.
func (o *Object) Fetch() (*rpb.RpbGetResp, error) {
	defer o.reset()
	opts := new(rpb.RpbGetReq)
	if o.opts != nil {
		if _, ok := o.opts.(*rpb.RpbGetReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be RpbGetReq")
		} else {
			opts = o.opts.(*rpb.RpbGetReq)
		}
	}
	opts.Bucket = []byte(o.bucket.name)
	opts.Key = []byte(o.key)
	in, err := proto.Marshal(opts)
	if err != nil {
		return nil, err
	}
	out, err := o.bucket.session.execute(Messages["GetReq"], in)
	if err != nil {
		return nil, err
	}
	o.vclock = out.(*rpb.RpbGetResp).Vclock
	return out.(*rpb.RpbGetResp), nil
}

// Store adds or replaces data for this object at key.
//
// It is up to the caller to make sure data is converted to []byte format.
func (o *Object) Store(data []byte) (*rpb.RpbPutResp, error) {
	defer o.reset()
	opts := new(rpb.RpbPutReq)
	if o.opts != nil {
		if _, ok := o.opts.(*rpb.RpbPutReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be RpbPutReq")
		} else {
			opts = o.opts.(*rpb.RpbPutReq)
		}
	}
	opts.Bucket = []byte(o.bucket.name)
	if o.key != "" {
		opts.Key = []byte(o.key)
	}
	if opts.Content == nil {
		opts.Content = &rpb.RpbContent{
			Value:       data,
			ContentType: o.ct,
		}
	} else {
		opts.Content.Value = data
	}
	if opts.Vclock == nil {
		opts.Vclock = o.vclock
	}
	in, err := proto.Marshal(opts)
	if err != nil {
		return nil, err
	}
	out, err := o.bucket.session.execute(Messages["PutReq"], in)
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbPutResp), nil
}

// Delete removes the both the data and key for this object.
func (o *Object) Delete() (bool, error) {
	defer o.reset()
	opts := new(rpb.RpbDelReq)
	if o.opts != nil {
		if _, ok := o.opts.(*rpb.RpbDelReq); !ok {
			return false, errors.New("Called Do() with wrong opts. Should be RpbDelReq")
		} else {
			opts = o.opts.(*rpb.RpbDelReq)
		}
	}
	opts.Bucket = []byte(o.bucket.name)
	opts.Key = []byte(o.key)
	if opts.Vclock == nil {
		opts.Vclock = o.vclock
	}
	in, err := proto.Marshal(opts)
	if err != nil {
		return false, err
	}
	out, err := o.bucket.session.execute(Messages["DelReq"], in)
	if err != nil {
		return false, err
	}
	return out.(bool), nil
}
