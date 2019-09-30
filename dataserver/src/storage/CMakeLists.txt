set(storage_SOURCES
    aggregate_calc.cpp
    field_value.cpp
    kv_fetcher.cpp
    meta_store.cpp
    metric.cpp
    row_decoder.cpp
    row_fetcher.cpp
    snapshot.cpp
    store.cpp
    store_txn.cpp
    util.cpp
    )

foreach(f IN LISTS storage_SOURCES)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"storage/${f}\"")
endforeach()

add_library(chubaodb-storage STATIC ${storage_SOURCES})
add_dependencies(chubaodb-storage build-3rd)
