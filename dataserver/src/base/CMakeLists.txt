set(base_SOURCES
    crc64.cpp
    fs_util.cpp
    histogram.cpp
    status.cpp
    system_info.cpp
    timer.cpp
    util.cpp
        )

if (APPLE)
    list(APPEND base_SOURCES system_info_mac.cpp)
elseif(UNIX)
    list(APPEND base_SOURCES system_info_linux.cpp)
else()
    message(FATAL_ERROR "Unsupported OS Type: " ${CMAKE_SYSTEM_NAME})
endif()

add_library(chubaodb-base STATIC ${base_SOURCES})
