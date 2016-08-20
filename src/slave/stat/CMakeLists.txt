cmake_minimum_required(VERSION 2.8) 

project(LabCryptoOrg-fence-slave-stat)

if (APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

set(BIN_INSTALL_DIR "/usr/bin/" CACHE PATH "")
set(HEADERS_INSTALL_DIR "/usr/include")
set(LIB_INSTALL_DIR "/usr/lib")
set(HOT_GENERATED_DIR ${PROJECT_SOURCE_DIR}/../../../hot/generated)

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

add_library(fence-slave-stat-lib SHARED 
  ${HOT_GENERATED_DIR}/fence/proxy/fence_monitor_service_proxy.cc
  ${HOT_GENERATED_DIR}/fence/proxy/fence_monitor_service_proxy_builder.cc
)

add_executable(fence-slave-stat
  ${PROJECT_SOURCE_DIR}/main.cc
)

target_link_libraries(fence-slave-stat
  fence-slave-stat-lib
  hotd
  pthread
)
