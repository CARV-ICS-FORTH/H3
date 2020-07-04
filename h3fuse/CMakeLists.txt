cmake_minimum_required(VERSION 3.10)

project(h3fuse VERSION 1.0 LANGUAGES C)

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
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -DDEBUG")
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

# https://gitlab.kitware.com/cmake/community/wikis/doc/ctest/Testing-With-CTest
enable_testing()
add_subdirectory(tests)

message(STATUS "Fuse version: ${FUSE_VERSION}" )


