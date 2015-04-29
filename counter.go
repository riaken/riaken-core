package riaken_core

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type Counter struct {
	bucket *Bucket // bucket this object is associated with
	key    string  // key this object is associated with
	opts   interface{}
}

func (c *Counter) reset() {
	c.opts = nil
}

func (c *Counter) GetOpts() interface{} {
	return c.opts
}

// Do allows opts to be passed to a method.  This call should be chained.
func (c *Counter) Do(opts interface{}) *Counter {
	c.opts = opts
	return c
}

// Update a counter.
func (c *Counter) Update(count int64) (*rpb.RpbCounterUpdateResp, error) {
	defer c.reset()
	opts := new(rpb.RpbCounterUpdateReq)
	if c.opts != nil {
		if _, ok := c.opts.(*rpb.RpbCounterUpdateReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be RpbCounterUpdateReq")
		} else {
			opts = c.opts.(*rpb.RpbCounterUpdateReq)
		}
	}
	opts.Bucket = []byte(c.bucket.name)
	opts.Key = []byte(c.key)
	opts.Amount = proto.Int64(count)
	in, err := proto.Marshal(opts)
	if err != nil {
		return nil, err
	}
	out, err := c.bucket.session.execute(Messages["CounterUpdateReq"], in)
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbCounterUpdateResp), nil
}

// Get a counter.
func (c *Counter) Get() (*rpb.RpbCounterGetResp, error) {
	defer c.reset()
	opts := new(rpb.RpbCounterGetReq)
	if c.opts != nil {
		if _, ok := c.opts.(*rpb.RpbCounterGetReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be RpbCounterGetReq")
		} else {
			opts = c.opts.(*rpb.RpbCounterGetReq)
		}
	}
	opts.Bucket = []byte(c.bucket.name)
	opts.Key = []byte(c.key)
	in, err := proto.Marshal(opts)
	if err != nil {
		return nil, err
	}
	out, err := c.bucket.session.execute(Messages["CounterGetReq"], in)
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbCounterGetResp), nil
}
