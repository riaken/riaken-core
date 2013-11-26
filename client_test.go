package riaken_core

import (
	"log"
	"testing"
	"time"
)

var client *Client

func Example() {
	// Riak cluster addresses
	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	// Create a client, passing the addresses, number of connections to maintain per cluster node, and the Dial timeout
	client := NewClient(addrs, 3, time.Second*2)
	// Dial the servers
	client.Dial()
	// Gracefully close all the connections when finished
	defer client.Close()

	// Grab a session to interact with the cluster
	session, err := client.Session()
	if err != nil {
		log.Print(err.Error())
	}
	// Hand the session back to the pool when finished
	defer session.Close()

	// Create a bucket association
	bucket := session.GetBucket("b1")
	// Prepare an object with a key
	object := bucket.Object("o1")
	// Store data
	_, err = object.Store([]byte("o1-data"))
	if err != nil {
		log.Print(err.Error())
	}

	// Fetch data
	data, err := object.Fetch()
	if err != nil {
		log.Print(err.Error())
	}
	// Fetch the content out of the returned data
	// Multiple instances mean multiple siblings were returned
	log.Print(string(data.GetContent()[0].GetValue()))

	// Delete the key and it's data from the cluster
	ok, err := object.Delete()
	if !ok {
		log.Print("deletion of object failed")
	}
	if err != nil {
		log.Print(err.Error())
	}
}

func dial() *Client {
	//addrs := []string{"127.0.0.1:8087"}
	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	client := NewClient(addrs, 3, time.Second*2)
	//client.debug = true
	client.Dial()
	return client
}

func TestNonExistentServers(t *testing.T) {
	addrs := []string{"127.0.0.1:9084", "127.0.0.1:9085", "127.0.0.1:9086", "127.0.0.1:9087", "127.0.0.1:9088"}
	client := NewClient(addrs, 3, time.Second*2)
	//client.debug = true
	client.Dial()
	_, err := client.Session()
	if err != nil {
		if err.Error() != "all nodes appear to be down" {
			t.Error("incorrect error for no available nodes")
		}
	}
}

func TestClientPing(t *testing.T) {
	client := dial()
	defer client.Close()
	session, err := client.Session()
	if err != nil {
		t.Error(err.Error())
	}
	defer session.Close()

	ping, err := session.Ping()
	if err != nil {
		t.Error(err.Error())
	}
	if !ping {
		t.Error("no ping response")
	}
}

func TestClientSetGetClientId(t *testing.T) {
	client := dial()
	defer client.Close()
	session, err := client.Session()
	if err != nil {
		t.Error(err.Error())
	}
	defer session.Close()

	if ok, err := session.SetClientId([]byte("client1")); !ok {
		t.Error("no response")
	} else if err != nil {
		t.Error(err.Error())
	}

	resp, err := session.GetClientId()
	if err != nil {
		t.Error(err.Error())
	}
	if string(resp.GetClientId()) != "client1" {
		t.Errorf("got: %s, expected: client1", string(resp.GetClientId()))
	}
}

func TestClientListBuckets(t *testing.T) {
	client := dial()
	defer client.Close()
	session, err := client.Session()
	if err != nil {
		t.Error(err.Error())
	}
	defer session.Close()

	buckets, err := session.ListBuckets()
	if err != nil {
		t.Error(err.Error())
	}
	if len(buckets) == 0 {
		t.Error("expected more than 0 buckets")
	}
}

func TestClientServerInfo(t *testing.T) {
	client := dial()
	defer client.Close()
	session, err := client.Session()
	if err != nil {
		t.Error(err.Error())
	}
	defer session.Close()

	info, err := session.ServerInfo()
	if err != nil {
		t.Error(err.Error())
	}
	t.Log(string(info.GetNode()))
	t.Log(string(info.GetServerVersion()))
}
