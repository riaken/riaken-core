proto:
	protoc --go_out=./rpb --proto_path=./rpb ./rpb/*.proto

test-prep:
	$(RIAK_ADMIN) bucket-type create test_counters '{"props":{"datatype":"counter"}}'
	$(RIAK_ADMIN) bucket-type create test_sets '{"props":{"datatype":"set"}}'
	$(RIAK_ADMIN) bucket-type create test_maps '{"props":{"datatype":"map"}}'
	@sleep 1
	$(RIAK_ADMIN) bucket-type activate test_counters
	$(RIAK_ADMIN) bucket-type activate test_sets
	$(RIAK_ADMIN) bucket-type activate test_maps

test:
	go test -v
