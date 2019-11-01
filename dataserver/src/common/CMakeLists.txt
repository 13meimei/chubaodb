set(common_SOURCES
    config_util.cpp
    server_config.cpp
    rocksdb_config.cpp
    ds_encoding.cpp
    masstree_env.cpp
    masstree_options.cpp
    rpc_request.cpp
    statistics.cpp
    string_util.cpp
    logger.cpp
)

foreach(f IN LISTS common_SOURCES)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"common/${f}\"")
endforeach()

add_library(chubaodb-common STATIC ${common_SOURCES})
add_dependencies(chubaodb-common build-3rd)
