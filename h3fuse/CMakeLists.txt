cmake_minimum_required(VERSION 3.10)

project(h3fuse VERSION 1.1 LANGUAGES C)

# Code checking
find_program(CMAKE_C_CPPCHECK NAMES cppcheck)

# The configuration file will be auto-generated into the binary tree i.e. build directory
# Note that the build directory has to be added as an include-dir
configure_file(h3fuse_config.h.in h3fuse_config.h)

# https://cmake.org/cmake/help/v3.10/variable/CMAKE_BUILD_TYPE.html?highlight=cmake_build_type
# https://cmake.org/cmake/help/v3.10/variable/CMAKE_CONFIGURATION_TYPES.html#variable:CMAKE_CONFIGURATION_TYPES
set(DEFAULT_BUILD_TYPE "Release")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

# Define GNU standard installation directories as variables CMAKE_INSTALL_xxxx
# https://cmake.org/cmake/help/v3.10/module/GNUInstallDirs.html
include(GNUInstallDirs)

#https://cmake.org/cmake/help/v3.10/variable/CMAKE_C_STANDARD.html?highlight=cmake_c_standard
set(CMAKE_C_STANDARD 11)

#https://gitlab.kitware.com/cmake/community/wikis/doc/cmake/Useful-Variables
#https://cmake.org/cmake/help/v3.10/command/add_compile_options.html?highlight=add_compile_options
add_compile_options(-Wall -Werror)
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -DDEBUG")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3")

#https://cmake.org/cmake/help/v3.10/command/add_library.html
set(SOURCE_FILES h3fuse.c )
add_executable(${PROJECT_NAME} ${SOURCE_FILES})

#https://cmake.org/cmake/help/v3.10/module/FindPkgConfig.html#command:pkg_search_module
find_package(PkgConfig)
pkg_search_module(FUSE REQUIRED fuse3)
pkg_search_module(GLIB REQUIRED glib-2.0)
target_include_directories(${PROJECT_NAME} PRIVATE ${FUSE_INCLUDE_DIRS} ${GLIB_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(${PROJECT_NAME} PRIVATE ${FUSE_LIBRARIES}  ${GLIB_LDFLAGS} h3lib)
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${FUSE_CFLAGS}")

message(STATUS "Fuse version: ${FUSE_VERSION}" )

install(TARGETS ${PROJECT_NAME})

set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_VENDOR "FORTH-ICS")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "An embedded object store filesystem")
set(CPACK_GENERATOR "RPM")
set(CPACK_RPM_PACKAGE_LICENSE "ASL v2.0")
set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
set(CPACK_RPM_PACKAGE_REQUIRES "glib2 >= 2.56, fuse3-libs >= 3.6.1, h3lib >= 1.1")
set(CPACK_RPM_PACKAGE_DESCRIPTION "H3 is an embedded High speed, High volume, and High availability object store, backed by a high-performance key-value store (Kreon, RocksDB, etc.) or a filesystem. H3 FUSE provides a filesystem using the h3lib library.")
#set(CPACK_GENERATOR "DEB")
#set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
#set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Antony Chazapis chazapis@ics.forth.gr")
set(CPACK_PACKAGING_INSTALL_PREFIX /usr)
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
include(CPack)
