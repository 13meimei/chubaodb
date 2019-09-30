set(GTEST_URL https://github.com/google/googletest.git)
set(GTEST_TAG release-1.8.0)
set(GTEST_INCLUDE_DIR ${EXTERNAL_DIR}/googletest/googletest/include)
set(GMOCK_INCLUDE_DIR ${EXTERNAL_DIR}/googletest/googlemock/include)
set(GTEST_LIBRARY ${EXTERNAL_DIR}/googletest/lib/libgtest.a)
if (NOT EXISTS ${GTEST_LIBRARY} OR NOT EXISTS ${GTEST_INCLUDE_DIR})
    ExternalProject_Add(gtest
            PREFIX gtest
            GIT_REPOSITORY ${GTEST_URL}
            GIT_TAG ${GTEST_TAG}
            SOURCE_DIR ${EXTERNAL_DIR}/googletest
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake .
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd gtest)
endif()
include_directories(${GTEST_INCLUDE_DIR})
include_directories(${GMOCK_INCLUDE_DIR})
