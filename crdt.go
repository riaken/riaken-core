package riaken_core

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/riaken/riaken-core/rpb"
)

type Crdt struct {
	bucket *Bucket // bucket this object is associated with
	key    string  // key this object is associated with
	opts   interface{}
}
