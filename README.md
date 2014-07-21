## Riaken

Go Protocol Buffer driver for the Riak distributed database

## Install

    go get http://gopkg.in/riaken/riaken-core.v1

## Documentation

http://godoc.org/gopkg.in/riaken/riaken-core.v1

## Extended Riaken

There are some modules which wrap/extend/test Riaken located at the following:

* https://github.com/riaken/riaken-struct - Wraps core with higher level struct functionality.
* https://github.com/riaken/riaken-test - Does integration testing against core and struct.

## Alternatives

For the record there are two existing mature Go PBC libraries.

* https://github.com/mrb/riakpbc - A collaboration between Michael Bernstein and myself.
* https://github.com/tpjg/goriakpbc - Ruby inspired, seems feature complete.

## Philosophy

The following points are what drive this project.

* Extendable.  riaken-core is the least common demoninator.  It is meant to be extended by projects like riaken-struct which add new behavior, such as the ability to convert high level struct data to/from JSON.
* Speed.  The current driver clocks at roughly 1800 ops/sec on a single 3.4 GHz Intel Core i7 iMac with 16gb of memory running 5 default Riak instances.  This should scale much higher against a real server cluster.

## Usage

### Client - Basic Initialization

The client is the all encompassing framework of riaken.  From there sessions should be grabbed and released as necessary.

	package main

	import "log"
	import "github.com/riaken/riaken-core"

	func main() {
		// Riak cluster addresses
		addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
		// Create a client, passing the addresses, and number of connections to maintain per cluster node
		client := riaken_core.NewClient(addrs, 1)
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
	}

### Client Operations

#### Ping

Simply see if the server is responsive.

	if !session.Ping() {
		log.Error("no ping response")
	}

#### Set Client ID

	if ok, err := session.SetClientId([]byte("client1")); !ok {
		log.Error("no response")
	} else if err != nil {
		log.Error(err.Error())
	}

#### Get Client ID

	res, err := session.GetClientId()
	if err != nil {
		log.Error(err.Error())
	}
	log.Print(string(res.GetClientId()))

#### List Buckets

**WARNING**: This is not recommended to be used against a production server.

	buckets, err := session.ListBuckets()
	if err != nil {
		log.Error(err.Error())
	}

#### Server Info

Get useful info about the Riak servers.

	info, err := session.ServerInfo()
	if err != nil {
		log.Error(err.Error())
	}
	log.Print(string(info.GetNode()))
	log.Print(string(info.GetServerVersion()))

### Bucket Operations

#### Set Bucket Properties

Not exactly straightforward because they require the use of the RPB structs.

	// Make sure to require this
	import (
		"github.com/riaken/riaken-core/rpb"
		"code.google.com/p/goprotobuf/proto"
	)

	// Code
	bucket := session.GetBucket("b2")
	props := &rpb.RpbBucketProps{
		NVal:      proto.Uint32(1),
		AllowMult: proto.Bool(true),
	}
	if ok, err := bucket.SetBucketProps(props); !ok {
		log.Error("could not set bucket props")
	} else if err != nil {
		log.Error(err.Error())
	}

#### Get Bucket Properties

	out, err := bucket.GetBucketProps()
	if err != nil {
		log.Error(err.Error())
	}
	if out.GetProps().GetAllowMult() != true {
		log.Errorf("expected: true, got: %t", out.GetProps().GetAllowMult())
	}

#### List Keys

**WARNING**: This is not recommended to be used against a production server.

Note that this is a streaming response and must be called until done.  If not streamed the next operation will fail and/or hang.

	var keys [][]byte
	// Loop until done is received from Riak
	for out, err := bucket.ListKeys(); !out.GetDone(); out, err = bucket.ListKeys() {
		if err != nil {
			log.Error(err.Error())
			break
		}
		keys = append(keys, out.GetKeys()...)
	}
	log.Print(keys)

### Object Operations

#### Store

Grab a bucket by name, an object with the key to store, and insert the data as `[]byte`.

	bucket := session.GetBucket("b1")
	object := bucket.Object("o1")
	if _, err := object.Store([]byte("o1-data")); err != nil {
		log.Error(err.Error())
	}

For server assigned keys.

	bucket := session.GetBucket("b1")
	object := bucket.Object("")                 // leave this blank
	res, err := object.Store([]byte("o1-data")) // fetch the response
	if err != nil {
		log.Error(err.Error())
	}
	log.Print(string(res.GetKey()))             // this is the server assigned key

#### Fetch

Note that the content comes back as an array.  If `> 1` this record has siblings.

	data, err := object.Fetch()
	if err != nil {
		log.Error(err.Error())
	}
	log.Print(string(data.GetContent()[0].GetValue()))

#### Delete

Verbose version.

	if ok, err := object.Delete(); !ok {
		log.Error("deletion of object failed")
	} else if err != nil {
		log.Error(err.Error())
	}

Simple version.

	if _, err := object.Delete(); err != nil {
		log.Error(err.Error())
	}

### Counter Operations

#### Update

	bucket := session.GetBucket("b5")
	counter := bucket.Counter("c1")
	if _, err := counter.Update(1); err != nil {
		log.Error(err.Error())
	}

#### Get

	data, err := counter.Get()
	if err != nil {
		t.Error(err.Error())
	}
	log.Print(data.GetValue())

### Query Operations

#### Map Reduce

Note that this is a streaming response and must be called until done.  If not streamed the next operation will fail and/or hang.

	request := []byte(`{
    	"inputs":"training",
    	"query":[{"map":{"language":"javascript",
    	"source":"function(riakObject) {
      		var val = riakObject.values[0].data.match(/pizza/g);
      		return [[riakObject.key, (val ? val.length : 0 )]];
    	}"}}]}`)
	contentType := []byte("application/json")

	// Query
	query := session.Query()
	var result []byte
	// Loop until done is received from Riak.
	for out, err := query.MapReduce(request, contentType); !out.GetDone(); out, err = query.MapReduce(request, contentType) {
		if err != nil {
			log.Error(err.Error())
			break
		}
		result = append(result, out.GetResponse()...)
	}
	log.Print(data.GetKeys())

### Secondary Indexes

Note that storage_backend must be set to riak_kv_eleveldb_backend in app.config to use this.

	query := session.Query()
	data, err := query.SecondaryIndexes([]byte("b1"), []byte("animal_bin"), []byte("chicken"), nil, nil, 0, nil)
	if err != nil {
		log.Error(err.Error())
	}

### Search

Note that riak_search needs to be enabled in the app.config to use this.

	query := session.Query()
	data, err := query.Search([]byte("b3"), []byte("food:pizza"))
	if err != nil {
		log.Error(err.Error())
	}
	log.Print(data.GetNumFound())

## Additional Complex Parameters

Sometimes it is desirable to pass more complex options to the server.  All methods capable of receiving additional options have access to `Do()`.  This method takes in a RPB struct and is chained together with the method one wishes to call.

	// Make sure to require this
	import (
		"github.com/riaken/riaken-core/rpb"
		"code.google.com/p/goprotobuf/proto"
	)

	// Code
	bucket := session.GetBucket("b1")
	object := bucket.Object("o1")

	// Store
	opts1 := &rpb.RpbPutReq{
		ReturnBody: proto.Bool(true),
	}
	ret, err := object.Do(opts1).Store([]byte("o1-data"))
	if err != nil {
		log.Error(err.Error())
	}
	if string(ret.GetContent()[0].GetValue()) != "o1-data" {
		log.Errorf("got %s, expected o1-data", string(ret.GetContent()[0].GetValue()))
	}

	// Fetch
	opts2 := &rpb.RpbGetReq{
		Head: proto.Bool(true),
	}
	data, err := object.Do(opts2).Fetch()
	if err != nil {
		log.Error(err.Error())
	}
	if len(data.GetContent()) > 0 {
		if string(data.GetContent()[0].GetValue()) != "" {
			log.Error("expected empty content")
		}
	}

	// Delete
	opts3 := &rpb.RpbDelReq{
		Rw: proto.Uint32(1),
	}
	if ok, err := object.Do(opts3).Delete(); !ok {
		log.Error("deletion of object failed")
	} else if err != nil {
		log.Error(err.Error())
	}

See the [Riak PBC docs](http://docs.basho.com/riak/latest/dev/references/protocol-buffers/) for a more detailed explanation of what all the parameters are for each method.

## Author

Brian Jones - mojobojo@gmail.com - https://twitter.com/mojobojo

Special thanks to Michael Bernstein.  This project would not have been possible without the blood and sweat of our collaboration on mrb/riakpbc.

## License

http://boj.mit-license.org/
