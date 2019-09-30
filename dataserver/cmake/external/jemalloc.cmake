set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCHUBAO_USE_JEMALLOC")
set(JEMALLOC_LIBRARY ${PROJECT_SOURCE_DIR}/third-party/jemalloc-5.2.0/lib/libjemalloc.a)
if (NOT EXISTS ${JEMALLOC_LIBRARY})
    ExternalProject_Add(jemalloc
            PREFIX jemalloc
            SOURCE_DIR ${PROJECT_SOURCE_DIR}/third-party/jemalloc-5.2.0
            CONFIGURE_COMMAND ./autogen.sh && ./configure --enable-prof --enable-shared=no --enable-stats
            --with-jemalloc-prefix=""
            BUILD_IN_SOURCE true
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd jemalloc)
endif()
include_directories(${PROJECT_SOURCE_DIR}/third-party/jemalloc-5.2.0/include)
