package riaken_core

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type CrdtCounter struct {
	crdt   *Crdt
	amount int64 // pending value
	Value  int64 // current value
}

// Increment counter.
func (c *CrdtCounter) Increment(amount int64) {
	c.amount = amount
}

// Decrement counter.
func (c *CrdtCounter) Decrement(amount int64) {
	c.amount = -amount
}

// Commit changes to database.
func (c *CrdtCounter) Commit() (*rpb.DtUpdateResp, error) {
	opts := &rpb.DtUpdateReq{
		Op: &rpb.DtOp{
			CounterOp: &rpb.CounterOp{
				Increment: proto.Int64(c.amount),
			},
		},
		ReturnBody: proto.Bool(true), // return so the latest value can be registered
	}
	res, err := c.crdt.Do(opts).Update()
	c.Value = res.GetCounterValue()
	return res, err
}

type CrdtSet struct {
	crdt    *Crdt
	adds    [][]byte
	removes [][]byte
	Values  []string // current values
}

// set [][]byte values to []string.
func (s *CrdtSet) set(values [][]byte) {
	for _, v := range values {
		s.Values = append(s.Values, string(v))
	}
}

// Add a new value to the set.
func (s *CrdtSet) Add(data string) {
	s.adds = append(s.adds, []byte(data))
	s.Values = append(s.Values, data)
}

// Remove a value from the set.
func (s *CrdtSet) Remove(data string) {
	s.removes = append(s.removes, []byte(data))
	for i, v := range s.Values {
		if v == data {
			s.Values = append(s.Values[:i], s.Values[i+1:]...)
			break
		}
	}
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
		ReturnBody: proto.Bool(true), // return so the latest value can be registered
	}
	res, err := s.crdt.Do(opts).Update()
	s.set(res.GetSetValue())
	s.adds = nil
	s.removes = nil
	return res, err
}

type CrdtMapType byte

const (
	CRDT_MAP_FLAG     CrdtMapType = 1
	CRDT_MAP_REGISTER CrdtMapType = 2
	CRDT_MAP_COUNTER  CrdtMapType = 3
	CRDT_MAP_SET      CrdtMapType = 4
	CRDT_MAP_MAP      CrdtMapType = 5
)

type CrdtMap struct {
	crdt      *Crdt
	remove    CrdtMapRemove
	Flags     map[string]bool
	Registers map[string]string
	Counters  map[string]*CrdtCounter
	Sets      map[string]*CrdtSet
	Maps      map[string]*CrdtMap
}

type CrdtMapRemove struct {
	Flags     []string
	Registers []string
	Counters  []string
	Sets      []string
	Maps      []string
}

// unpack data from database.
func (m *CrdtMap) unpack(dt *Crdt, mes []*rpb.MapEntry) {
	m.crdt = dt
	for _, me := range mes {
		name := string(me.GetField().GetName())
		switch me.GetField().GetType() {
		case rpb.MapField_FLAG:
			m.Flags[name] = me.GetFlagValue()
		case rpb.MapField_REGISTER:
			m.Registers[name] = string(me.GetRegisterValue())
		case rpb.MapField_COUNTER:
			m.Counters[name] = dt.NewCounter()
			m.Counters[name].Value = me.GetCounterValue()
		case rpb.MapField_SET:
			m.Sets[name] = dt.NewSet()
			m.Sets[name].set(me.GetSetValue())
		case rpb.MapField_MAP:
			m.Maps[name] = dt.NewMap()
			m.Maps[name].unpack(dt, me.GetMapValue())
		}
	}
}

// removes returns the list of fields to remove from the Map.
func (m *CrdtMap) removes() []*rpb.MapField {
	out := []*rpb.MapField{}
	for _, v := range m.remove.Flags {
		t := rpb.MapField_FLAG
		r := &rpb.MapField{
			Name: []byte(v),
			Type: &t,
		}
		out = append(out, r)
	}
	for _, v := range m.remove.Registers {
		t := rpb.MapField_REGISTER
		r := &rpb.MapField{
			Name: []byte(v),
			Type: &t,
		}
		out = append(out, r)
	}
	for _, v := range m.remove.Counters {
		t := rpb.MapField_COUNTER
		r := &rpb.MapField{
			Name: []byte(v),
			Type: &t,
		}
		out = append(out, r)
	}
	for _, v := range m.remove.Sets {
		t := rpb.MapField_SET
		r := &rpb.MapField{
			Name: []byte(v),
			Type: &t,
		}
		out = append(out, r)
	}
	for _, v := range m.remove.Maps {
		t := rpb.MapField_MAP
		r := &rpb.MapField{
			Name: []byte(v),
			Type: &t,
		}
		out = append(out, r)
	}
	return out
}

// pack data to send to database.
func (m *CrdtMap) pack() []*rpb.MapUpdate {
	out := []*rpb.MapUpdate{}
	for k, v := range m.Flags {
		t := rpb.MapField_FLAG
		flag := &rpb.MapUpdate{
			Field: &rpb.MapField{
				Name: []byte(k),
				Type: &t,
			},
		}
		var o rpb.MapUpdate_FlagOp
		if v {
			o = rpb.MapUpdate_ENABLE
		} else {
			o = rpb.MapUpdate_DISABLE
		}
		flag.FlagOp = &o
		out = append(out, flag)
	}

	for k, v := range m.Registers {
		t := rpb.MapField_REGISTER
		register := &rpb.MapUpdate{
			Field: &rpb.MapField{
				Name: []byte(k),
				Type: &t,
			},
			RegisterOp: []byte(v),
		}
		out = append(out, register)
	}

	for k, v := range m.Counters {
		t := rpb.MapField_COUNTER
		counter := &rpb.MapUpdate{
			Field: &rpb.MapField{
				Name: []byte(k),
				Type: &t,
			},
			CounterOp: &rpb.CounterOp{
				Increment: proto.Int64(v.amount),
			},
		}
		out = append(out, counter)
	}

	for k, v := range m.Sets {
		t := rpb.MapField_SET
		set := &rpb.MapUpdate{
			Field: &rpb.MapField{
				Name: []byte(k),
				Type: &t,
			},
			SetOp: &rpb.SetOp{
				Adds:    v.adds,
				Removes: v.removes,
			},
		}
		out = append(out, set)
	}

	for k, v := range m.Maps {
		t := rpb.MapField_MAP
		mp := &rpb.MapUpdate{
			Field: &rpb.MapField{
				Name: []byte(k),
				Type: &t,
			},
			MapOp: &rpb.MapOp{
				Removes: v.removes(),
				Updates: v.pack(),
			},
		}
		out = append(out, mp)
	}
	return out
}

// Remove field with name and type t.
func (m *CrdtMap) Remove(t CrdtMapType, name string) {
	switch t {
	case CRDT_MAP_FLAG:
		m.remove.Flags = append(m.remove.Flags, name)
		delete(m.Flags, name)
	case CRDT_MAP_REGISTER:
		m.remove.Registers = append(m.remove.Registers, name)
		delete(m.Registers, name)
	case CRDT_MAP_COUNTER:
		m.remove.Counters = append(m.remove.Counters, name)
		delete(m.Counters, name)
	case CRDT_MAP_SET:
		m.remove.Sets = append(m.remove.Sets, name)
		delete(m.Sets, name)
	case CRDT_MAP_MAP:
		m.remove.Maps = append(m.remove.Maps, name)
		delete(m.Maps, name)
	}
}

// Commit changes to the database.
func (m *CrdtMap) Commit() (*rpb.DtUpdateResp, error) {
	opts := &rpb.DtUpdateReq{
		Op: &rpb.DtOp{
			MapOp: &rpb.MapOp{
				Removes: m.removes(),
				Updates: m.pack(),
			},
		},
		ReturnBody: proto.Bool(true), // return so the latest value can be registered
	}
	res, err := m.crdt.Do(opts).Update()
	m.unpack(m.crdt, res.GetMapValue())
	m.remove = CrdtMapRemove{} // reset
	return res, err
}

// Crdt can manually take raw opts via Do() and execute Fetch() or Update(),
// however, it is preferred to call the Counter(), Set(), or Map() methods
// and work with each object through those interfaces.
type Crdt struct {
	bucket  *Bucket      // bucket this object is associated with
	key     string       // key this object is associated with
	context []byte       // dt context
	opts    interface{}  // rpb.Dt* options
	Counter *CrdtCounter // counter result for this object
	Set     *CrdtSet     // set result for this object
	Map     *CrdtMap     // map result for this object
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

// NewCounter returns a new CRDT Counter.
func (dt *Crdt) NewCounter() *CrdtCounter {
	return &CrdtCounter{
		crdt: dt,
	}
}

// NewSet returns a new CRDT Set.
func (dt *Crdt) NewSet() *CrdtSet {
	return &CrdtSet{
		crdt: dt,
	}
}

// NewMap returns a new CRDT Map.
func (dt *Crdt) NewMap() *CrdtMap {
	return &CrdtMap{
		crdt:      dt,
		Flags:     make(map[string]bool),
		Registers: make(map[string]string),
		Counters:  make(map[string]*CrdtCounter),
		Sets:      make(map[string]*CrdtSet),
		Maps:      make(map[string]*CrdtMap),
	}
}

func (dt *Crdt) processCounter(res int64) {
	dt.Counter = dt.NewCounter()
	dt.Counter.Value = res
}

func (dt *Crdt) processSet(res [][]byte) {
	dt.Set = dt.NewSet()
	dt.Set.set(res)
}

func (dt *Crdt) processMap(res []*rpb.MapEntry) {
	dt.Map = dt.NewMap()
	dt.Map.unpack(dt, res)
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
	dt.processCounter(out.(*rpb.DtFetchResp).GetValue().GetCounterValue())
	dt.processSet(out.(*rpb.DtFetchResp).GetValue().GetSetValue())
	dt.processMap(out.(*rpb.DtFetchResp).GetValue().GetMapValue())
	return out.(*rpb.DtFetchResp), nil
}

// Update adds or replaces data for this object.
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
	dt.processCounter(out.(*rpb.DtUpdateResp).GetCounterValue())
	dt.processSet(out.(*rpb.DtUpdateResp).GetSetValue())
	dt.processMap(out.(*rpb.DtUpdateResp).GetMapValue())
	return out.(*rpb.DtUpdateResp), nil
}
