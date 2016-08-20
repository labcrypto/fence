cmake_minimum_required(VERSION 2.8) 

project(LabCryptoOrg-fence-master)

if (APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

set(BIN_INSTALL_DIR "/usr/bin/" CACHE PATH "")
set(HEADERS_INSTALL_DIR "/usr/include")
set(LIB_INSTALL_DIR "/usr/lib")
set(HOT_GENERATED_DIR ${PROJECT_SOURCE_DIR}/../../hot/generated)

link_directories("/usr/local/lib")
if (APPLE)
  link_directories("/opt/local/lib/")
endif()

include_directories(${HOT_GENERATED_DIR})

if (DEBUG)
  set(CMAKE_CXX_FLAGS "-std=c++11 -O0 -g")
else()
  set(CMAKE_CXX_FLAGS "-std=c++11 -O3")
endif()

if (APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-writable-strings -Wno-deprecated-register -Wno-unused-function")
elseif (UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-attributes -Wno-write-strings -Wno-unused-but-set-variable -Wno-sign-compare -Wno-unused-function")
endif()

add_subdirectory(ack)
add_subdirectory(pop)
add_subdirectory(enq)
add_subdirectory(stat)
add_subdirectory(sack)
add_subdirectory(spop)

add_library(fence-master-fence-lib SHARED
  ${HOT_GENERATED_DIR}/fence/message.cc
  ${HOT_GENERATED_DIR}/fence/service/fence_service_request_handler.cc
  ${HOT_GENERATED_DIR}/fence/service/fence_monitor_service_request_handler.cc
)

add_library(fence-master-transport-lib SHARED
  ${HOT_GENERATED_DIR}/transport/enqueue_report.cc
  ${HOT_GENERATED_DIR}/transport/transport_message.cc
  ${HOT_GENERATED_DIR}/transport/service/transport_service_request_handler.cc
  ${HOT_GENERATED_DIR}/transport/service/transport_monitor_service_request_handler.cc
)

add_library(fence-master-lib SHARED
  ${PROJECT_SOURCE_DIR}/runtime.cc
  ${PROJECT_SOURCE_DIR}/fence_service_impl.cc
  ${PROJECT_SOURCE_DIR}/fence_monitor_service_impl.cc
  ${PROJECT_SOURCE_DIR}/transport_service_impl.cc
  ${PROJECT_SOURCE_DIR}/transport_monitor_service_impl.cc
  ${PROJECT_SOURCE_DIR}/master_thread.cc
)

add_executable(fence-master
  ${PROJECT_SOURCE_DIR}/main.cc
)

target_link_libraries(fence-master
  fence-master-lib
  fence-master-fence-lib
  fence-master-transport-lib
  hotd
  pthread
  abettor-c-fs
  abettor-cc-conf
  abettor-cc-string
  abettor-cc-os
  abettor-cc-date
)

install(TARGETS fence-master-lib DESTINATION ${LIB_INSTALL_DIR})
install(TARGETS fence-master-fence-lib DESTINATION ${LIB_INSTALL_DIR})
install(TARGETS fence-master-transport-lib DESTINATION ${LIB_INSTALL_DIR})
install(TARGETS fence-master DESTINATION ${BIN_INSTALL_DIR})