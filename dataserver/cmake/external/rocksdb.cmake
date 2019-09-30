set(ROCKSDB_URL https://github.com/facebook/rocksdb)
set(ROCKSDB_TAG v5.11.3)
set(ROCKSDB_INCLUDE_DIR ${EXTERNAL_DIR}/rocksdb/include)
set(ROCKSDB_LIBRARY ${EXTERNAL_DIR}/rocksdb/librocksdb.a)
if (NOT EXISTS ${ROCKSDB_LIBRARY} OR NOT EXISTS ${ROCKSDB_INCLUDE_DIR})
    ExternalProject_Add(rocksdb
            PREFIX rocksdb
            GIT_REPOSITORY ${ROCKSDB_URL}
            GIT_TAG ${ROCKSDB_TAG}
            SOURCE_DIR ${EXTERNAL_DIR}/rocksdb
            CONFIGURE_COMMAND cmake . -DWITH_TESTS=OFF -DPORTABLE=ON -DWITH_TOOLS=OFF
            BUILD_IN_SOURCE 1
            BUILD_BYPRODUCTS ${ROCKSDB_LIBRARY}
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd rocksdb)
endif()
include_directories(${ROCKSDB_INCLUDE_DIR})
