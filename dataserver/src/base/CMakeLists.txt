set(base_SOURCES
    crc64.cpp
    fs_util.cpp
    histogram.cpp
    status.cpp
    system_info.cpp
    timer.cpp
    util.cpp
)

add_library(chubaodb-base STATIC ${base_SOURCES})
add_dependencies(chubaodb-base build-3rd)
