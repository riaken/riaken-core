package riaken_core

import (
	"errors"

	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type CrdtCounter struct {
	crdt  *Crdt // parent
	Value int64 // current value
}

func (c *CrdtCounter) execute(amount int64) (*rpb.DtUpdateResp, error) {
	opts := &rpb.DtUpdateReq{
		Op: &rpb.DtOp{
			CounterOp: &rpb.CounterOp{
				Increment: proto.Int64(amount),
			},
		},
	}
	res, err := c.crdt.Do(opts).Update()
	c.Value = res.GetCounterValue()
	return res, err
}

func (c *CrdtCounter) Increment(amount int64) (*rpb.DtUpdateResp, error) {
	if amount < 0 {
		amount *= -1
	}
	return c.execute(amount)
}

func (c *CrdtCounter) Decrement(amount int64) (*rpb.DtUpdateResp, error) {
	if amount > 0 {
		amount *= -1
	}
	return c.execute(amount)
}

type CrdtSet struct {
	crdt    *Crdt // parent
	adds    [][]byte
	removes [][]byte
	Values  [][]byte // current values
}

// Add a new value to the set.
func (s *CrdtSet) Add(data []byte) {
	s.adds = append(s.adds, data)
}

// Remove a value from the set.
func (s *CrdtSet) Remove(data []byte) {
	s.removes = append(s.removes, data)
}

// Commit changes to the database.
func (s *CrdtSet) Commit() (*rpb.DtUpdateResp, error) {
	opts := &rpb.DtUpdateReq{
		Op: &rpb.DtOp{
			SetOp: &rpb.SetOp{
				Adds:    s.adds,
				Removes: s.removes,
			},
		},
	}
	res, err := s.crdt.Do(opts).Update()
	s.Values = res.GetSetValue()
	s.adds = nil
	s.removes = nil
	return res, err
}

type CrdtMap struct {
	crdt *Crdt // parent
}

func (m *CrdtMap) Commit() (*rpb.DtUpdateResp, error) {
	opts := &rpb.DtUpdateReq{
		Op: &rpb.DtOp{
			MapOp: &rpb.MapOp{},
		},
	}
	return m.crdt.Do(opts).Update()
}

// Crdt can manually take raw opts via Do() and execute Fetch() or Store(),
// however, it is preferred to call the Counter(), Set(), or Map() methods
// and work with each object through those interfaces.
type Crdt struct {
	bucket  *Bucket // bucket this object is associated with
	key     string  // key this object is associated with
	context []byte  // dt context
	opts    interface{}
}

func (dt *Crdt) reset() {
	dt.context = nil
	dt.opts = nil
}

func (dt *Crdt) GetOpts() interface{} {
	return dt.opts
}

// Do allows opts to be passed to a method.  This call should be chained.
func (dt *Crdt) Do(opts interface{}) *Crdt {
	dt.opts = opts
	return dt
}

func (dt *Crdt) Counter() *CrdtCounter {
	return &CrdtCounter{
		crdt: dt,
	}
}

func (dt *Crdt) Set() *CrdtSet {
	return &CrdtSet{
		crdt: dt,
	}
}

func (dt *Crdt) Map() *CrdtMap {
	return &CrdtMap{
		crdt: dt,
	}
}

// Fetch returns the data for this object at key.
func (dt *Crdt) Fetch() (*rpb.DtFetchResp, error) {
	defer dt.reset()
	opts := new(rpb.DtFetchReq)
	if dt.opts != nil {
		if _, ok := dt.opts.(*rpb.DtFetchReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be DtFetchReq")
		} else {
			opts = dt.opts.(*rpb.DtFetchReq)
		}
	}
	opts.Bucket = []byte(dt.bucket.name)
	opts.Key = []byte(dt.key)
	if opts.Type == nil {
		opts.Type = dt.bucket.btype
	}
	in, err := proto.Marshal(opts)
	if err != nil {
		return nil, err
	}
	out, err := dt.bucket.session.execute(Messages["DtFetchReq"], in)
	if err != nil {
		return nil, err
	}
	dt.context = out.(*rpb.DtFetchResp).Context
	return out.(*rpb.DtFetchResp), nil
}

// Store adds or replaces data for this object.
func (dt *Crdt) Update() (*rpb.DtUpdateResp, error) {
	defer dt.reset()
	opts := new(rpb.DtUpdateReq)
	if dt.opts != nil {
		if _, ok := dt.opts.(*rpb.DtUpdateReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be DtUpdateReq")
		} else {
			opts = dt.opts.(*rpb.DtUpdateReq)
		}
	}
	opts.Bucket = []byte(dt.bucket.name)
	opts.Key = []byte(dt.key)
	if opts.Type == nil {
		opts.Type = dt.bucket.btype
	}
	if opts.Context == nil {
		opts.Context = dt.context
	}
	in, err := proto.Marshal(opts)
	if err != nil {
		return nil, err
	}
	out, err := dt.bucket.session.execute(Messages["DtUpdateReq"], in)
	if err != nil {
		return nil, err
	}
	return out.(*rpb.DtUpdateResp), nil
}
