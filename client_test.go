package riaken_core

import (
	"log"
	"testing"
)

var client *Client

func Example() {
	// Riak cluster addresses
	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	// Create a client, passing the addresses, number of connections to maintain per cluster node
	client := NewClient(addrs, 1)
	// Dial the servers
	if err := client.Dial(); err != nil {
		log.Fatal(err.Error()) // all nodes are down
	}
	// Gracefully close all the connections when finished
	defer client.Close()

	// Grab a session to interact with the cluster
	session := client.Session()
	// Release the session
	defer session.Release()

	// Create a bucket association
	bucket := session.GetBucket("b1")
	// Prepare an object with a key
	object := bucket.Object("o1")
	// Store data
	if _, err := object.Store([]byte("o1-data")); err != nil {
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
	client := NewClient(addrs, 1)
	//client.Debug(true)
	if err := client.Dial(); err != nil {
		log.Fatal(err.Error()) // all nodes are down
	}
	return client
}

/*func TestNonExistentServers(t *testing.T) {
	addrs := []string{"127.0.0.1:9084", "127.0.0.1:9085", "127.0.0.1:9086", "127.0.0.1:9087", "127.0.0.1:9088"}
	client := NewClient(addrs, 1)
	defer client.Close()
	err := client.Dial()
	if err.Error() != "all nodes appear to be down" {
		t.Log("expected all nodes to be down")
	}
}*/

func TestClientPing(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()
	if !session.Ping() {
		t.Error("no ping response")
	}
}

func TestClientSetGetClientId(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

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
	session := client.Session()
	defer session.Release()

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
	session := client.Session()
	defer session.Release()

	info, err := session.ServerInfo()
	if err != nil {
		t.Error(err.Error())
	}
	t.Log(string(info.GetNode()))
	t.Log(string(info.GetServerVersion()))
}
