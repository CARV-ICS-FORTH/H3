cmake_minimum_required(VERSION 3.10)

project(h3lib VERSION 1.0 LANGUAGES C)

# The configuration file will be auto-generated into the binary tree i.e. build directory
configure_file(h3lib_config.h.in h3lib_config.h)


#set(DEFAULT_BUILD_TYPE "Release")
#
#if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
#  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
#  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
#  # Set the possible values of build type for cmake-gui
#  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release")
#endif()

include(GNUInstallDirs)



#https://cmake.org/cmake/help/v3.10/command/add_library.html
set(SOURCE_FILES h3lib.c kvlib.c kvlib_fs.c )
add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES})

#https://cmake.org/cmake/help/v3.10/command/target_include_directories.html
target_include_directories( ${PROJECT_NAME} PUBLIC
                            "${PROJECT_BINARY_DIR}")

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


# https://cmake.org/cmake/help/v3.10/command/install.html
#install(TARGETS ${PROJECT_NAME} LIBRARY MyLibConfig)

#install(TARGETS ${PROJECT_NAME} EXPORT MyLibConfig
#    ARCHIVE  DESTINATION ${CMAKE_INSTALL_LIBDIR}
#    LIBRARY  DESTINATION ${CMAKE_INSTALL_LIBDIR}
#    RUNTIME  DESTINATION ${CMAKE_INSTALL_BINDIR})
#    
#install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME})
#
#install(EXPORT MyLibConfig DESTINATION share/MyLib/cmake)
#
#export(TARGETS ${PROJECT_NAME} FILE MyLibConfig.cmake)





# set the project name and version
#project(h3lib  VERSION 1.0)

#configure_file(h3lib_config.h.in h3lib_config.h)

# add the executable
#add_executable(h3lib main.c)
#
#target_include_directories(h3lib PUBLIC "${PROJECT_BINARY_DIR}" )