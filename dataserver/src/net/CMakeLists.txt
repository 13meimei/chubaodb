set(net_SOURCES
    context_pool.cpp
    message.cpp
    protocol.cpp
    server.cpp
    session.cpp
    )

foreach(f IN LISTS net_SOURCES) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"net/${f}\"") 
endforeach() 

add_library(chubaodb-net STATIC ${net_SOURCES})
add_dependencies(chubaodb-net build-3rd)
