cmake_minimum_required(VERSION 2.8) 

project(LabCryptoOrg-fence)

if (APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

set(BIN_INSTALL_DIR "/usr/bin/" CACHE PATH "")
set(HEADERS_INSTALL_DIR "/usr/include")
set(LIB_INSTALL_DIR "/usr/lib")

link_directories("/usr/local/lib")
if (APPLE)
  link_directories("/opt/local/lib/")
endif()

add_subdirectory(src/master)
add_subdirectory(src/slave)
