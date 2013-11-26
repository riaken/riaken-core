package riaken_core

import (
	"log"
	"time"
)

// Pool contains a small pool of connections to a single Riak node, allowing for concurrent network connections up to max.
type Pool struct {
	addr    string        // address this node pool is associated with
	max     int           // the max number of connections to allow per node
	timeout time.Duration // default timeout
	nodes   chan *Node    // channel of nodes to pull from
	debug   bool          // debugging info
}

// NewPool creates a connection pool to a node at addr, with a total of max connections, and a Dial timeout.
func NewPool(addr string, max int, timeout time.Duration) *Pool {
	return &Pool{
		addr:    addr,
		max:     max,
		timeout: timeout,
		nodes:   make(chan *Node, max),
	}
}

// Dial attempts to connect to the associated Riak node and create count default instances.
func (p *Pool) Dial() {
	for i := 0; i < p.max; i++ {
		if p.debug {
			log.Printf("pool dialing: %s", p.addr)
		}
		node := NewNode(p.addr)
		node.debug = p.debug
		err := node.Dial(p.timeout)
		if err != nil {
			if p.debug {
				log.Print(err.Error())
			}
		} else {
			if p.debug {
				log.Printf("connected to: %s", node.addr)
			}
		}
		p.nodes <- node
	}
}

// Close gracefully closes all the node connections.
func (p *Pool) Close() {
	for i := 0; i < p.max; i++ {
		node := <-p.nodes
		if p.debug {
			log.Printf("closing connection: %s", node.addr)
		}
		node.Close()
		p.nodes <- node
	}
}

// Get retrieves a node from the pool.
func (p *Pool) Get() *Node {
	return <-p.nodes
}

// Put returns a node back to the pool.
func (p *Pool) Put(node *Node) {
	p.nodes <- node
}
