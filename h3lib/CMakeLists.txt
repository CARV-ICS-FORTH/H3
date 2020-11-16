cmake_minimum_required(VERSION 3.10)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

project(h3lib VERSION 1.1 LANGUAGES C)

# Code checking
find_program(CMAKE_C_CPPCHECK NAMES cppcheck)
# if (CMAKE_C_CPPCHECK)
#     list(
#         APPEND CMAKE_C_CPPCHECK
#             "--enable=all"
#     )
# endif()

# The configuration file will be auto-generated into the binary tree i.e. build directory
configure_file(h3lib_config.h.in h3lib_config.h)


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
#add_compile_options(-Wall -Wno-uninitialized -Wno-unused-result -Werror)
#add_compile_options(-Wall -Wno-uninitialized -Wno-unused-result -Wl,--no-as-needed) <-- libs are considered irrespective of the order they are presented to the compiler/linker
add_compile_options(-Wall -Wno-uninitialized -Wno-unused-result)

SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -DDEBUG")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3")


find_package(PkgConfig REQUIRED)
pkg_search_module(GLIB REQUIRED glib-2.0)

find_package(rocksdb)
find_package(kreon)
find_package(hiredis)

#https://cmake.org/cmake/help/v3.10/command/add_library.html
set(SOURCE_FILES h3lib.c bucket.c object.c multipart.c kv_fs.c util.c url_parser.c)
if(ROCKSDB_FOUND)
	set(SOURCE_FILES ${SOURCE_FILES} kv_rocksdb.c)
	add_definitions(-DH3LIB_USE_ROCKSDB)
endif()

if(KREON_FOUND)
	set(SOURCE_FILES ${SOURCE_FILES} kv_kreon.c)
	add_definitions(-DH3LIB_USE_KREON)
endif()

if(HIREDIS_FOUND)
  set(SOURCE_FILES ${SOURCE_FILES} kv_redis.c)
  add_definitions(-DH3LIB_USE_REDIS)
endif()

add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES})

#https://cmake.org/cmake/help/v3.10/command/target_include_directories.html
target_include_directories( ${PROJECT_NAME} PUBLIC "${PROJECT_BINARY_DIR}" "${GLIB_HEADERS}")
target_include_directories(${PROJECT_NAME}  PRIVATE ${GLIB_INCLUDE_DIRS})
target_link_libraries(${PROJECT_NAME} PRIVATE ${GLIB_LDFLAGS} uuid)

if(ROCKSDB_FOUND)
	target_include_directories(${PROJECT_NAME} PRIVATE ${ROCKSDB_INCLUDE_DIR})
	target_link_libraries(${PROJECT_NAME} PRIVATE ${ROCKSDB_LIBRARIES} dl)
endif()

if(KREON_FOUND)
	target_include_directories(${PROJECT_NAME} PRIVATE ${KREON_INCLUDE_DIR})
	target_link_libraries(${PROJECT_NAME} PRIVATE ${KREON_LIBRARIES} zookeeper_mt rdmacm ibverbs log)
endif()

if(HIREDIS_FOUND)
  target_include_directories(${PROJECT_NAME} PRIVATE ${HIREDIS_INCLUDE_DIR})
  target_link_libraries(${PROJECT_NAME} PRIVATE ${HIREDIS_LIBRARIES})
endif()

if(H3LIB_USE_COMPRESSION)
  find_library(ZSTD_LIBRARY zstd REQUIRED)
  message(STATUS "Zstandard found")
  add_definitions(-DH3LIB_USE_COMPRESSION)
  target_link_libraries(${PROJECT_NAME} PRIVATE ${ZSTD_LIBRARY})
endif()

#target_include_directories( ${PROJECT_NAME} PUBLIC
#                            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
#                            $<INSTALL_INTERFACE:include>
#                            PRIVATE src)
#

# https://cmake.org/cmake/help/v3.10/command/set_target_properties.html
# https://cmake.org/cmake/help/v3.10/prop_tgt/SOVERSION.html
set_target_properties( ${PROJECT_NAME} PROPERTIES
                       VERSION ${PROJECT_VERSION}
                       SOVERSION 1)

# Use "sudo make install" to apply.
# https://cmake.org/cmake/help/v3.10/command/install.html
install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES h3lib.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME} )


set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_VENDOR "FORTH-ICS")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "An embedded object store")
set(CPACK_GENERATOR "RPM")
set(CPACK_RPM_PACKAGE_LICENSE "ASL v2.0")
set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
set(CPACK_RPM_PACKAGE_REQUIRES "glib2 >= 2.56, libuuid >= 2.23")
set(CPACK_RPM_PACKAGE_DESCRIPTION "H3 is an embedded High speed, High volume, and High availability object store, backed by a high-performance key-value store (Kreon, RocksDB, etc.) or a filesystem. H3 is implemented in the h3lib library, which provides a cloud-friendly API, similar to Amazon's S3. Python and Java wrappers are also available.")
#set(CPACK_GENERATOR "DEB")
#set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
#set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Antony Chazapis chazapis@ics.forth.gr")
set(CPACK_PACKAGING_INSTALL_PREFIX /usr)
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
include(CPack)
