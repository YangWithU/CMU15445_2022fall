set(SQLLOGICTEST_SOURCES sqllogictest.cpp parser.cpp)
add_executable(sqllogictest ${SQLLOGICTEST_SOURCES})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstandalone-debug")

target_link_libraries(sqllogictest bustub)
set_target_properties(sqllogictest PROPERTIES OUTPUT_NAME bustub-sqllogictest)
