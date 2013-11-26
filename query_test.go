package riaken_core

import (
	"os/exec"
	"regexp"
	"testing"
)

import (
	"github.com/riaken/riaken-core/rpb"
)

// Example from http://docs.basho.com/riak/latest/dev/using/mapreduce/

func TestMapReduce(t *testing.T) {
	client := dial()
	defer client.Close()
	session, err := client.Session()
	if err != nil {
		t.Error(err.Error())
	}
	defer session.Close()

	// Test Data
	bucket := session.GetBucket("training")
	object1 := bucket.Object("foo")
	_, err = object1.Store([]byte("pizza data goes here"))
	if err != nil {
		t.Error(err.Error())
	}

	object2 := bucket.Object("bar")
	_, err = object2.Store([]byte("pizza pizza pizza pizza"))
	if err != nil {
		t.Error(err.Error())
	}

	object3 := bucket.Object("baz")
	_, err = object3.Store([]byte("nothing to see here"))
	if err != nil {
		t.Error(err.Error())
	}

	object4 := bucket.Object("bam")
	_, err = object4.Store([]byte("pizza pizza pizza"))
	if err != nil {
		t.Error(err.Error())
	}

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
			t.Error(err.Error())
			break
		}
		result = append(result, out.GetResponse()...)
	}

	// [["foo",1],["baz",0],["bar",4],["bam",3]]
	m, err := regexp.MatchString(`["foo",1]`, string(result))
	if err != nil {
		t.Error(err.Error())
	}
	if !m {
		t.Error("Mismatched foo result")
	}
	m, err = regexp.MatchString(`["bar",4]`, string(result))
	if err != nil {
		t.Error(err.Error())
	}
	if !m {
		t.Error("Mismatched bar result")
	}
	m, err = regexp.MatchString(`["baz",0]`, string(result))
	if err != nil {
		t.Error(err.Error())
	}
	if !m {
		t.Error("Mismatched baz result")
	}
	m, err = regexp.MatchString(`["bam",3]`, string(result))
	if err != nil {
		t.Error(err.Error())
	}
	if !m {
		t.Error("Mismatched bam result")
	}

	// Cleanup
	if _, err := object1.Delete(); err != nil {
		t.Error(err.Error())
	}
	if _, err := object2.Delete(); err != nil {
		t.Error(err.Error())
	}
	if _, err := object3.Delete(); err != nil {
		t.Error(err.Error())
	}
	if _, err := object4.Delete(); err != nil {
		t.Error(err.Error())
	}
}

func TestSecondaryIndexes(t *testing.T) {
	client := dial()
	defer client.Close()
	session, err := client.Session()
	if err != nil {
		t.Error(err.Error())
	}
	defer session.Close()

	// Setup
	bucket := session.GetBucket("b1")
	object := bucket.Object("o1")
	opts := &rpb.RpbPutReq{
		Content: &rpb.RpbContent{
			Indexes: []*rpb.RpbPair{
				&rpb.RpbPair{
					Key:   []byte("animal_bin"),
					Value: []byte("chicken"),
				},
			},
		},
	}
	if _, err := object.Do(opts).Store([]byte("o1-data")); err != nil {
		t.Error(err.Error())
	}

	// Query
	query := session.Query()
	data, err := query.SecondaryIndexes([]byte("b1"), []byte("animal_bin"), []byte("chicken"), nil, nil, 0, nil)
	if err != nil {
		t.Error(err.Error())
	}

	if len(data.GetKeys()) == 0 {
		t.Error("expected results")
	} else {
		if string(data.GetKeys()[0]) != "o1" {
			t.Error("expected first key to be o1")
		}
	}

	// Cleanup
	if _, err := object.Delete(); err != nil {
		t.Error(err.Error())
	}
}

func TestSearch(t *testing.T) {
	client := dial()
	defer client.Close()
	session, err := client.Session()
	if err != nil {
		t.Error(err.Error())
	}
	defer session.Close()

	// Set bucket properties.
	// Unfortunately these still aren't exposed via PBC, so do it manually with curl.
	_, err = exec.Command("curl", "-XPUT", "-H", "content-type:application/json", "http://127.0.0.1:8093/riak/b3", "-d", `{"props":{"precommit":[{"mod":"riak_search_kv_hook","fun":"precommit"}]}}`).Output()
	if err != nil {
		t.Error(err.Error())
	}

	// Setup
	bucket := session.GetBucket("b3")
	object := bucket.Object("o1")
	object.ContentType([]byte("application/json"))
	if _, err := object.Store([]byte(`{"food": "pizza"}`)); err != nil {
		t.Error(err.Error())
	}

	// Query
	query := session.Query()
	data, err := query.Search([]byte("b3"), []byte("food:pizza"))
	if err != nil {
		t.Error(err.Error())
	}

	if data.GetNumFound() == 0 {
		t.Error("expected results")
	}

	// Cleanup
	if _, err := object.Delete(); err != nil {
		t.Error(err.Error())
	}
}
