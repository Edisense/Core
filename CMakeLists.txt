cmake_minimum_required(VERSION 2.8.11)
project(EdisenseCore)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
# Find includes in corresponding build directories
#set(CMAKE_INCLUDE_CURRENT_DIR ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(Threads REQUIRED)
find_package(SQLite REQUIRED)

include_directories(src include)
include_directories(${SQLITE3_INCLUDE_DIR})

set(CORE_INCLUDE include/command.h
                 include/edisense_types.h
                 include/state.h
                 include/state.cpp)

set(CORE_PARTITION src/partition/partition_table.h
                   src/partition/partition_table.cpp
                   src/partition/partition_db.h
                   src/partition/partition_db.cpp
                   src/partition/partition_io.h
                   src/partition/partition_io.cpp
                   src/partition/hash.h
                   src/partition/hash.cpp)

set(CORE_SERVER src/server/server-internal.h
                src/server/server-internal.cpp
                src/server/config.h
                src/server/config.cpp)

set(CORE_UTIL src/util/socket-util.h
              src/util/socket-util.cpp 
              src/util/rw_lock.h
              src/util/rw_lock.cpp)

set(SOURCE_FILES main.cpp
                 ${CORE_INCLUDE}
                 ${CORE_PARTITION}
                 ${CORE_SERVER}
                 ${CORE_UTIL})

add_executable(EdisenseCore ${SOURCE_FILES})

target_link_libraries(EdisenseCore ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(EdisenseCore ${SQLITE3_LIBRARIES})

