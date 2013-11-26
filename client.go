package riaken_core

import (
	"container/ring"
	"errors"
	"log"
	"time"
)

type Client struct {
	cluster *ring.Ring // a cluster of node pools which this client is associated with
	debug   bool       // toggle debug output
}

// NewClient takes a list of Riak node addresses to connect to, the max number of connections to maintain per node, and Dial timeout.
func NewClient(addrs []string, max int, timeout time.Duration) *Client {
	client := new(Client)
	client.cluster = ring.New(len(addrs))
	for _, addr := range addrs {
		pool := NewPool(addr, max, timeout)
		client.cluster.Value = pool
		client.cluster = client.cluster.Next()
	}
	return client
}

// Dial connects the client to all the nodes in the cluster.
func (c *Client) Dial() {
	if c.debug {
		log.Printf("dialing %d servers...", c.cluster.Len())
	}

	for elem, i := c.cluster, 0; i < c.cluster.Len(); elem, i = elem.Next(), i+1 {
		pool := elem.Value.(*Pool)
		pool.debug = c.debug
		if c.debug {
			log.Printf("attempting to connect to %s", pool.addr)
		}
		pool.Dial()
	}
}

// Close gracefully shuts down all the node connections.
func (c *Client) Close() {
	for elem, i := c.cluster, 0; i < c.cluster.Len(); elem, i = elem.Next(), i+1 {
		pool := elem.Value.(*Pool)
		pool.Close()
	}
}

// Session returns a new session with a node from the cluster pool.
//
// Currently uses a naive round robin method.
func (c *Client) Session() (*Session, error) {
	for elem, i := c.cluster, 0; i < c.cluster.Len(); elem, i = elem.Next(), i+1 {
		pool := elem.Value.(*Pool)
		node := pool.Get()
		if node.active {
			session := NewSession(c, node)
			return session, nil
		}
		pool.Put(node)
	}
	return nil, errors.New("all nodes appear to be down")
}

// Release returns the node to it's pool.
func (c *Client) Release(node *Node) {
	for elem, i := c.cluster, 0; i < c.cluster.Len(); elem, i = elem.Next(), i+1 {
		pool := elem.Value.(*Pool)
		if pool.addr == node.addr {
			pool.Put(node)
			break
		}
	}
}
