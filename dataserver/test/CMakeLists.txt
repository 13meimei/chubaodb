include_directories(${PROJECT_SOURCE_DIR}/test)
include_directories(${PROJECT_SOURCE_DIR}/third-party/sql-parser/src)

set(test_helper_SRCS
    helper/mock/range_context_mock.cpp
    helper/mock/rpc_request_mock.cpp
    helper/helper_util.cpp
    helper/request_builder.cpp
    helper/sql_parser.cpp
    helper/query_parser.cpp
    helper/range_test_fixture.cpp
    helper/store_test_fixture.cpp
    helper/table.cpp
)

foreach(f IN LISTS test_helper_SRCS)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()

add_library(chubaodb-test-helper SHARED ${test_helper_SRCS})
if (APPLE)
    target_link_libraries(chubaodb-test-helper chubaodb-ds-shared sqlparser ${GTEST_LIBRARY})
endif()

set (test_DEPEND_LIBS
    chubaodb-test-helper
    chubaodb-ds-shared
    ${depend_LIBRARYS}
    sqlparser
    ${GTEST_LIBRARY}
)

set(test_SRCS
    unittest/checkpoint_unittest.cpp
    unittest/db_unittest.cpp
    unittest/encoding_unittest.cpp
    unittest/field_value_unittest.cpp
    unittest/mass_tree_unittest.cpp
    unittest/meta_store_unittest.cpp
    unittest/monitor_unittest.cpp
    unittest/range_ddl_unittest.cpp
    unittest/range_meta_unittest.cpp
    unittest/range_kv_unittest.cpp
    unittest/range_sql_unittest.cpp
    unittest/range_tree_unittest.cpp
    unittest/scaner_unittest.cpp
    unittest/status_unittest.cpp
    unittest/store_unittest.cpp
    unittest/store_txn_unittest.cpp
    unittest/timer_unittest.cpp
    unittest/util_unittest.cpp
    unittest/wal_unittest.cpp
)

foreach(f IN LISTS test_SRCS)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()

foreach(testfile ${test_SRCS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${test_DEPEND_LIBS})
    if("${exename}" MATCHES "unittest")
        add_test(NAME ${exename} COMMAND ${exename})
    endif()
endforeach(testfile ${TESTS})
