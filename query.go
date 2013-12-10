package riaken_core

import (
	"errors"
)

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type Query struct {
	// session reference
	session     *Session
	streamState int // track state of streaming
	opts        interface{}
}

func (q *Query) reset() {
	q.opts = nil
}

// Do allows opts to be passed to a method.  This call should be chained.
func (q *Query) Do(opts interface{}) *Query {
	q.opts = opts
	return q
}

// MapReduce query.
//
// This uses a streaming interface and should be called repeatedly until done is true.
//
//	var result []byte
//	// Loop until done is received from Riak.
//	for out, err := query.MapReduce(request, contentType); !out.GetDone(); out, err = query.MapReduce(request, contentType) {
//		if err != nil {
//			t.Error(err.Error())
//			break
//		}
//		result = append(result, out.GetResponse()...)
//	}
func (q *Query) MapReduce(req, ct []byte) (*rpb.RpbMapRedResp, error) {
	opts := &rpb.RpbMapRedReq{
		Request:     req,
		ContentType: ct,
	}
	var err error
	var out interface{}
	switch q.streamState {
	case 0:
		in, err := proto.Marshal(opts)
		if err != nil {
			return nil, err
		}
		out, err = q.session.execute(23, in) // RpbMapRedReq
		if err != nil {
			return nil, err
		}
		q.streamState = 1

		// Fall through and do an initial read as well
	case 1:
		out, err = q.session.executeRead()
		if err != nil {
			return nil, err
		}
	}
	if out.(*rpb.RpbMapRedResp).GetDone() {
		q.streamState = 0
	}
	return out.(*rpb.RpbMapRedResp), nil
}

// SecondaryIndexes fetches a set of keys that matches a 2i index.
//
// Optional: This can use a streaming interface and should be called repeatedly until done is true.
// Set stream to true when calling Do(RpbIndexReq).SecondaryIndexes().
//
// Note: storage_backend must be set to riak_kv_eleveldb_backend in app.config.
func (q *Query) SecondaryIndexes(bucket, index, key, start, end []byte, maxResults uint32, continuation []byte) (*rpb.RpbIndexResp, error) {
	defer q.reset()
	opts := &rpb.RpbIndexReq{}
	if q.opts != nil {
		if _, ok := q.opts.(*rpb.RpbIndexReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be RpbIndexReq")
		} else {
			opts = q.opts.(*rpb.RpbIndexReq)
		}
	}
	opts.Bucket = bucket
	opts.Index = index
	if maxResults > 0 {
		opts.MaxResults = &maxResults
	}
	opts.Continuation = continuation
	var qType rpb.RpbIndexReq_IndexQueryType
	if string(key) != "" {
		qType = 0
		opts.Qtype = &qType
		opts.Key = key
	} else {
		qType = 1
		opts.Qtype = &qType
		opts.RangeMin = start
		opts.RangeMax = end
	}
	var err error
	var out interface{}
	switch q.streamState {
	case 0:
		in, err := proto.Marshal(opts)
		if err != nil {
			return nil, err
		}
		out, err = q.session.execute(25, in) // RpbIndexReq
		if err != nil {
			return nil, err
		}
		q.streamState = 1
	case 1:
		out, err = q.session.executeRead()
		if err != nil {
			return nil, err
		}
	}
	if out.(*rpb.RpbIndexResp).GetDone() {
		q.streamState = 0
	}
	return out.(*rpb.RpbIndexResp), nil
}

// Search retrieves a list of documents.
//
// Note: riak_search may need to be enabled in app.config.
func (q *Query) Search(index, query []byte) (*rpb.RpbSearchQueryResp, error) {
	defer q.reset()
	opts := new(rpb.RpbSearchQueryReq)
	if q.opts != nil {
		if _, ok := q.opts.(*rpb.RpbSearchQueryReq); !ok {
			return nil, errors.New("Called Do() with wrong opts. Should be RpbSearchQueryReq")
		} else {
			opts = q.opts.(*rpb.RpbSearchQueryReq)
		}
	}
	opts.Q = query
	opts.Index = index
	in, err := proto.Marshal(opts)
	if err != nil {
		return nil, err
	}
	out, err := q.session.execute(27, in) // RpbSearchQueryReq
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbSearchQueryResp), nil
}
