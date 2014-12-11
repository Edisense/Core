cmake_minimum_required(VERSION 2.8.11)
project(EdisenseCore)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")

find_package(Threads REQUIRED)
find_package(SQLite REQUIRED)

add_subdirectory(ext/Comms)

include_directories(src include)
include_directories(ext/Comms/include)
include_directories(${SQLITE3_INCLUDE_DIR})

set(CORE_INCLUDE include/command.h
                 ext/Comms/include/edisense_types.h
                 include/state.h
                 include/global.h
                 src/server/state.cpp
                 src/server/global.cpp)

set(CORE_PARTITION src/partition/partition_table.h
                   src/partition/partition_table.cpp
                   src/partition/partition_db.h
                   src/partition/partition_db.cpp
                   src/partition/partition_io.h
                   src/partition/partition_io.cpp)

set(CORE_BLE  src/ble/ble_client_internal.h
              src/ble/ble_client_internal.cpp)

set(CORE_DAEMONS  src/daemons/daemons.h
                  src/daemons/daemons.cpp
                  src/daemons/db_file_transfer.h
                  src/daemons/db_file_transfer.cpp
                  src/daemons/rebalance_log.h
                  src/daemons/rebalance_log.cpp)

set(CORE_SERVER src/server/server_internal.h
                src/server/server_internal.cpp)

set(CORE_UTIL src/util/socket-util.h
              src/util/socket-util.cpp 
              src/util/rw_lock.h
              src/util/hash.h
              src/util/utilization.h)

set(SOURCE_FILES main.cpp
                 ${CORE_INCLUDE}
                 ${CORE_PARTITION}
                 ${CORE_DAEMONS}
                 ${CORE_BLE}
                 ${CORE_SERVER}
                 ${CORE_UTIL})

add_executable(EdisenseCore ${SOURCE_FILES})


target_link_libraries(EdisenseCore ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(EdisenseCore ${SQLITE3_LIBRARIES})
target_link_libraries(EdisenseCore zmqpp)
target_link_libraries(EdisenseCore EdisenseComms)



add_executable(PartitionGenerator generate_partition_table.cpp
                                  src/util/hash.h
                                  src/partition/partition_io.h
                                  src/partition/partition_io.cpp)

target_link_libraries(PartitionGenerator ${SQLITE3_LIBRARIES})