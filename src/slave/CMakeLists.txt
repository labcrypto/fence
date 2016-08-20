cmake_minimum_required(VERSION 2.8) 

project(LabCryptoOrg-fence-slave)

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
add_subdirectory(stat)
add_subdirectory(mstat)
add_subdirectory(enq)

add_library(fence-slave-fence-lib SHARED
  ${HOT_GENERATED_DIR}/fence/message.cc
  ${HOT_GENERATED_DIR}/fence/service/fence_service_request_handler.cc
  ${HOT_GENERATED_DIR}/fence/service/fence_monitor_service_request_handler.cc
  ${HOT_GENERATED_DIR}/fence/service/fence_test_service_request_handler.cc
)

add_library(fence-slave-transport-lib SHARED
  ${HOT_GENERATED_DIR}/transport/enqueue_report.cc
  ${HOT_GENERATED_DIR}/transport/transport_message.cc
  ${HOT_GENERATED_DIR}/transport/proxy/transport_service_proxy.cc
  ${HOT_GENERATED_DIR}/transport/proxy/transport_service_proxy_builder.cc
)

add_library(fence-slave-lib SHARED
  ${PROJECT_SOURCE_DIR}/runtime.cc
  ${PROJECT_SOURCE_DIR}/fence_service_impl.cc
  ${PROJECT_SOURCE_DIR}/fence_monitor_service_impl.cc
  ${PROJECT_SOURCE_DIR}/fence_test_service_impl.cc
  ${PROJECT_SOURCE_DIR}/slave_thread.cc
)

add_executable(fence-slave
  ${PROJECT_SOURCE_DIR}/main.cc
)

target_link_libraries(fence-slave
  fence-slave-lib
  fence-slave-fence-lib
  fence-slave-transport-lib
  hotd
  pthread
  abettor-c-fs
  abettor-cc-conf
  abettor-cc-string
  abettor-cc-os
  abettor-cc-date
)

install(TARGETS fence-slave-lib DESTINATION ${LIB_INSTALL_DIR})
install(TARGETS fence-slave-fence-lib DESTINATION ${LIB_INSTALL_DIR})
install(TARGETS fence-slave-transport-lib DESTINATION ${LIB_INSTALL_DIR})
install(TARGETS fence-slave DESTINATION ${BIN_INSTALL_DIR})