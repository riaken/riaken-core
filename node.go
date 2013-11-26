package riaken_core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

const (
	NODE_STATE_ACTIVE   int = 0
	NODE_STATE_SHUTDOWN int = 0
)

// Node is a very simple construct which simply reads and writes data from/to the Riak node connection.
type Node struct {
	addr   string        // address this node is associated with
	conn   net.Conn      // connection
	active bool          // whether connection is active or not
	debug  bool          // debugging info
	wait   time.Duration // time to wait until a new check is performed
	state  int           // track shutdown state
}

// NewNode creates a new node associated with address.
func NewNode(addr string) *Node {
	return &Node{
		addr:  addr,
		wait:  time.Second * 60, // set default to wait 60 seconds
		state: NODE_STATE_ACTIVE,
	}
}

// Dial attempts to connect to the Riak node with a timeout.
func (n *Node) Dial(timeout time.Duration) error {
	var err error
	n.conn, err = net.DialTimeout("tcp", n.addr, timeout)
	if err != nil {
		if n.debug {
			log.Print(err.Error())
		}
		return err
	}
	n.active = true

	go n.check()

	return nil
}

// Close closes the underlying net connection.
func (n *Node) Close() {
	if n.active {
		n.state = NODE_STATE_SHUTDOWN
		n.active = false
		n.conn.Close()
	}
}

// check runs in the background and makes sure the node remains connected.  If for some reason
// it disconnects, it will attempt to redial, backing off over time until a successful connection is made.
func (n *Node) check() {
	time.Sleep(n.wait)

	if !n.active {
		var err error
		n.conn, err = net.DialTimeout("tcp", n.addr, n.wait)
		if err != nil {
			n.wait *= 2 // double the wait time for each failure
		} else {
			n.active = true
		}
	}

	if n.state == NODE_STATE_ACTIVE {
		go n.check()
	}
}

// read response from the network connection.
func (n *Node) read() ([]byte, error) {
	buf := make([]byte, 4)
	var size int32
	// first 4 bytes are always size of message
	if count, err := io.ReadFull(n.conn, buf); err == nil && count == 4 {
		sbuf := bytes.NewBuffer(buf)
		binary.Read(sbuf, binary.BigEndian, &size)
		data := make([]byte, size)
		// read rest of message and return it if no errors
		if count, err := io.ReadFull(n.conn, data); err == nil && count == int(size) {
			return data, nil
		} else if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// write data to network connection.
func (n *Node) write(data []byte) error {
	count, err := n.conn.Write(data)
	if err == nil && count == len(data) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New(fmt.Sprintf("data length: %d, only wrote: %d", len(data), count))
}
