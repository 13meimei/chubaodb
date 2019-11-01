set (proto_SOURCES
    gen/dspb/admin.pb.cc
    gen/dspb/api.pb.cc
    gen/dspb/error.pb.cc
    gen/dspb/expr.pb.cc
    gen/dspb/function.pb.cc
    gen/dspb/kv.pb.cc
    gen/dspb/raft_internal.pb.cc
    gen/dspb/schedule.pb.cc
    gen/dspb/txn.pb.cc
    gen/dspb/processorpb.pb.cc
    gen/basepb/basepb.pb.cc
    gen/mspb/mspb.pb.cc
)
add_library(chubaodb-proto STATIC ${proto_SOURCES})
add_dependencies(chubaodb-proto build-3rd)
