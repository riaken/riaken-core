package riaken_core

import (
	"errors"
	"log"
	"time"
)

const PING_RATE time.Duration = time.Second * 5

var ErrAllNodesDown error = errors.New("all nodes appear to be down")

type Client struct {
	cluster chan *Session
	debug   bool // toggle debug output
}

// NewClient takes a list of Riak node addresses to connect to and the max number of connections to maintain per node.
func NewClient(addrs []string, max int) *Client {
	client := &Client{
		cluster: make(chan *Session, len(addrs)*max),
	}
	for _, addr := range addrs {
		for i := 0; i < max; i++ {
			client.cluster <- NewSession(client, addr)
		}
	}
	return client
}

func (c *Client) Debug(debug bool) {
	c.debug = debug
}

// Dial connects the client to all the nodes in the cluster.
// Nodes which are down at startup will attempt to dial later.
// If all nodes are down an error will be thrown.
func (c *Client) Dial() error {
	down := 0
	for i := 0; i < len(c.cluster); i++ {
		s := <-c.cluster
		s.debug = c.debug
		if err := s.Dial(); err != nil {
			down++
			if c.debug {
				log.Print(err.Error())
			}
		}
		c.cluster <- s
	}
	if down == len(c.cluster) {
		return ErrAllNodesDown
	}
	go c.check()
	return nil
}

// check runs a Riak Ping on each connection, and redials if the connection was lost.
func (c *Client) check() {
	for {
		select {
		case <-time.After(PING_RATE):
			for i := 0; i < len(c.cluster); i++ {
				go func() {
					s := <-c.cluster
					if !s.Available() {
						s.check()
					}
					c.cluster <- s
				}()
			}
		}
	}
}

// Close gracefully shuts down all the node connections.
func (c *Client) Close() {
	for i := 0; i < len(c.cluster); i++ {
		go func() {
			s := <-c.cluster
			s.Close()
			c.cluster <- s
		}()
	}
}

// Session returns a new session.
func (c *Client) Session() *Session {
	for {
		s := <-c.cluster
		if s.Available() {
			return s
		}
		c.cluster <- s
	}
	return nil
}

func (c *Client) release(s *Session) {
	c.cluster <- s
}
