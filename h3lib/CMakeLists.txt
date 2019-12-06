cmake_minimum_required(VERSION 3.10)

project(h3lib VERSION 1.0 LANGUAGES C)

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
add_compile_options(-Wall)
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3")


#https://cmake.org/cmake/help/v3.10/command/add_library.html
set(SOURCE_FILES h3lib.c bucket.c kv_fs.c )
add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES})

#https://cmake.org/cmake/help/v3.10/command/target_include_directories.html
target_include_directories( ${PROJECT_NAME} PUBLIC
                            "${PROJECT_BINARY_DIR}"
                            "${GLIB_HEADERS}")
                            

find_package(PkgConfig REQUIRED)
pkg_search_module(GLIB REQUIRED glib-2.0)
target_include_directories(${PROJECT_NAME} PRIVATE ${GLIB_INCLUDE_DIRS})
target_link_libraries(${PROJECT_NAME} INTERFACE ${GLIB_LDFLAGS})
                           

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

# https://gitlab.kitware.com/cmake/community/wikis/doc/ctest/Testing-With-CTest
enable_testing()
add_subdirectory(tests)

