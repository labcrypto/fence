cmake_minimum_required(VERSION 2.8) 

project(fence)

if (APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

set(BIN_INSTALL_DIR "/usr/bin/" CACHE PATH "")
set(HEADERS_INSTALL_DIR "/usr/local/include")
set(LIB_INSTALL_DIR "/usr/local/lib")

link_directories("/usr/local/lib")
if (APPLE)
  link_directories("/opt/local/lib/")
endif()

execute_process(COMMAND ./gen.sh
  COMMAND rm -rf ./generated
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/hot
)

add_subdirectory(src/master fence-master-bin)
add_subdirectory(src/slave fence-slave-bin)
