package riaken_core

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type Session struct {
	Client *Client // reference back to client
	Node   *Node   // the Node this session has access to for it's duration
}

func NewSession(client *Client, Node *Node) *Session {
	return &Session{
		Client: client,
		Node:   Node,
	}
}

// execute does the full request/response cycle on a command using a single Node connection instance.
func (s *Session) execute(code int, in []byte) (interface{}, error) {
	req, err := rpbWrite(code, in)
	if err != nil {
		return nil, err
	}

	if err := s.Node.write(req); err != nil {
		return nil, err
	}

	resp, err := s.Node.read()
	if err != nil {
		return nil, err
	}

	data, err := rpbRead(resp)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// executeRead continues to read streaming value from the connection.
func (s *Session) executeRead() (interface{}, error) {
	resp, err := s.Node.read()
	if err != nil {
		return nil, err
	}

	data, err := rpbRead(resp)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetBucket returns a new bucket to interact with on this session.
func (s *Session) GetBucket(name string) *Bucket {
	return &Bucket{
		session: s,
		name:    name,
	}
}

// Query returns a new query interface on this session.
func (s *Session) Query() *Query {
	return &Query{
		session: s,
	}
}

// Close releases the Node back to the client pool.
func (s *Session) Close() {
	s.Client.Release(s.Node)
}

// ListBuckets returns a list of buckets from Riak.
//
// Riak Docs - Caution: This call can be expensive for the server - do not use in performance sensitive code.
func (s *Session) ListBuckets() ([]*Bucket, error) {
	out, err := s.execute(15, nil) // RpbListBucketsReq
	if err != nil {
		return nil, err
	}
	blist := out.(*rpb.RpbListBucketsResp).GetBuckets()
	buckets := make([]*Bucket, len(blist))
	for i, name := range blist {
		buckets[i] = s.GetBucket(string(name))
	}
	return buckets, nil
}

// Ping is a server method which returns a Riak ping response.
func (s *Session) Ping() (bool, error) {
	out, err := s.execute(1, nil) // RpbPingReq
	if err != nil {
		return false, err
	}
	return out.(bool), nil
}

// GetClientId gets the id set for this client.
func (s *Session) GetClientId() (*rpb.RpbGetClientIdResp, error) {
	out, err := s.execute(3, nil) // RpbGetClientIdReq
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbGetClientIdResp), nil
}

// SetClientId sets the id for this client.
func (s *Session) SetClientId(id []byte) (bool, error) {
	opt := &rpb.RpbSetClientIdReq{
		ClientId: id,
	}
	in, err := proto.Marshal(opt)
	if err != nil {
		return false, err
	}
	out, err := s.execute(5, in) // RpbSetClientIdReq
	if err != nil {
		return false, err
	}
	return out.(bool), nil
}

// ServerInfo is a method which returns the information for the Riak cluster.
func (s *Session) ServerInfo() (*rpb.RpbGetServerInfoResp, error) {
	out, err := s.execute(7, nil) // RpbGetServerInfoReq
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbGetServerInfoResp), nil
}
