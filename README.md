## Riaken

Go Protocol Buffer driver for the Riak distributed database

## Install

### Development

    go get github.com/riaken/riaken-core

### 2.0.x Compatible

### 1.4.x Compatible

    go get http://gopkg.in/riaken/riaken-core.v1

## Documentation

### Development

http://godoc.org/github.com/riaken/riaken-core

### 2.0.x

### 1.4.x

http://godoc.org/gopkg.in/riaken/riaken-core.v1

## Extended Riaken

There are some modules which wrap/extend/test Riaken located at the following:

* https://github.com/riaken/riaken-struct - Wraps core with higher level struct functionality.
* https://github.com/riaken/riaken-test - Does integration testing against core and struct.

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

Buckets now have a recommended Type() method which allows for another level of namespacing.

    bucket := session.GetBucket("bucket").Type("high_level")

The type is set to `default` by Riak if not specified.

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

#### Set Bucket Type

	bucket := session.GetBucket("b2").Type("test_maps")
	props := &rpb.RpbBucketProps{
		AllowMult: proto.Bool(true),
	}
	if ok, err := bucket.SetBucketType(props); !ok {
		t.Error("could not set bucket props")
	} else if err != nil {
		t.Error(err.Error())
	}

#### Reset Bucket

	bucket := session.GetBucket("b2").Type("test_maps")
	if ok, err := bucket.ResetBucket(); !ok {
		t.Error("could not set bucket props")
	} else if err != nil {
		t.Error(err.Error())
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

### CRDTs

CRDTs can be queried similar to an Object.

	crdt := bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}

They will then have their Counter, Set, or Map value set depending on what was initially stored in Riak.

    // crdt.Counter
    // crdt.Set
    // crdt.Map

They are simply deleted with the Object interface.

	object := bucket.Object("foo")
	if _, err := object.Delete(); err != nil {
		t.Fatal(err.Error())
	}

#### Counters

	bucket := session.GetBucket("crdt_counter").Type("test_counters")
	counter := bucket.Crdt("foo").NewCounter()
	counter.Increment(4)
	if _, err := counter.Commit(); err != nil {
		t.Fatal(err.Error())
	}
	counter.Decrement(1)
	if _, err := counter.Commit(); err != nil {
		t.Fatal(err.Error())
	}
	log.Print(counter.Value) // should equal 3

#### Sets

	bucket := session.GetBucket("crdt_set").Type("test_sets")
	set := bucket.Crdt("foo").NewSet()
	set.Add("bar")
	set.Add("baz")
	if _, err := set.Commit(); err != nil {
		t.Fatal(err.Error())
	}
	log.Print(set.Values) // should contain ["bar", "baz"]
	set.Remove("baz")
	set.Add("car")
	if _, err := set.Commit(); err != nil {
		t.Fatal(err.Error())
	}
	log.Print(set.Values) // should contain ["bar", "car"]

#### Maps

Maps contain:

* Flags
* Registers
* Counters
* Sets
* Maps

They are used in the following way:

	bucket := session.GetBucket("crdt_map").Type("test_maps")
	crdt := bucket.Crdt("foo")
	mp := crdt.NewMap()

	// Add Flags
	mp.Flags["f1"] = true
	mp.Flags["f2"] = false

	// Add Registers
	mp.Registers["r1"] = "1r"
	mp.Registers["r2"] = "2r"

	// Add Counters
	mp.Counters["c1"] = crdt.NewCounter()
	mp.Counters["c1"].Increment(10)

	// Add Sets
	mp.Sets["s1"] = crdt.NewSet()
	mp.Sets["s1"].Add("1")
	mp.Sets["s1"].Add("2")
	mp.Sets["s1"].Add("3")
	mp.Sets["s2"] = crdt.NewSet()
	mp.Sets["s2"].Add("a")
	mp.Sets["s2"].Add("b")

	// Add Maps (within Maps, within...)
	mp.Maps["m1"] = crdt.NewMap()
	mp.Maps["m1"].Flags["ff1"] = true
	mp.Maps["m1"].Registers["rr1"] = "1rr"
	mp.Maps["m1"].Counters["cc1"] = crdt.NewCounter()
	mp.Maps["m1"].Counters["cc1"].Increment(20)
	mp.Maps["m1"].Sets["ss1"] = crdt.NewSet()
	mp.Maps["m1"].Sets["ss1"].Add("111")
	mp.Maps["m1"].Sets["ss1"].Add("222")
	mp.Maps["m1"].Maps["mm1"] = crdt.NewMap()
	mp.Maps["m1"].Maps["mm1"].Flags["fff1"] = false

	// Save
	if _, err := mp.Commit(); err != nil {
		t.Fatal(err.Error())
	}

Values can be removed from Maps with Remove():

	crdt := bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}
    crdt.Remove(CRDT_MAP_FLAG, "f2")
	if _, err := mp.Commit(); err != nil {
		t.Fatal(err.Error())
	}
    // crdt.Map.Flags["f2"] should no longer exist

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

## License

http://boj.mit-license.org/
