## Riaken

Go Protocol Buffer driver for the Riak distributed database

## Install

    go get github.com/riaken/riaken-core

## Documentation

http://godoc.org/github.com/riaken/riaken-core

## Extended Riaken

There are some modules which wrap/extend/test Riaken located at the following:

* https://github.com/riaken/riaken-struct - Wraps core with higher level struct functionality.
* https://github.com/riaken/riaken-test - Does integration testing against core and struct.

## Alternatives

For the record there are two existing mature Go PBC libraries.

* https://github.com/mrb/riakpbc - I helped write a good chunk of this one, it's pretty solid.
* https://github.com/tpjg/goriakpbc - Ruby inspired, seems feature complete.

## Philosophy

The following points are what drive this project.

* Simple. The code base is built to remain as straightforward as possible.  Rather than complex mutex locks, sessions are simply passed around over a channel.  This allows sessions to remain stateful, and not conflict with things such as background connectivity checks.
* Extendable.  riaken-core is the least common demoninator.  It is meant to be extended by projects like riaken-struct which add new behavior, such as the ability to convert high level struct data to/from JSON.
* Speed.  The current driver clocks at roughly 1800 ops/sec on a single 3.4 GHz Intel Core i7 iMac with 16gb of memory running 5 default Riak instances.  This should clearly scale higher against a real server cluster.

### Author

Brian Jones - mojobojo@gmail.com

