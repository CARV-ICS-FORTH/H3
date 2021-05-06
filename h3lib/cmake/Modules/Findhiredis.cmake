#https://gitlab.kitware.com/cmake/community/-/wikis/doc/tutorials/How-To-Find-Libraries

# - Try to find Redis client (HIREDIS)
# Once done this will define
#  HIREDIS_FOUND - System has Redis client
#  HIREDIS_INCLUDE_DIRS - The client's include directories
#  HIREDIS_LIBRARIES - The libraries needed to use the client
#  HIREDIS_DEFINITIONS - Compiler switches required for using the client


# Use pkg-config to detect include/library paths hiredis
find_package(PkgConfig)
pkg_check_modules(PC_HIREDIS QUIET hiredis)
set(HIREDIS_DEFINITIONS ${PC_HIREDIS_CFLAGS_OTHER})


# Dependencies use plural forms, the package itself uses the singular forms defined by find_path and find_library
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h
          HINTS ${PC_HIREDIS_INCLUDEDIR} ${PC_HIREDIS_INCLUDE_DIRS}
          PATH_SUFFIXES include )

find_library(HIREDIS_LIBRARIES hiredis
             HINTS ${PC_HIREDIS_LIBDIR} ${PC_HIREDIS_LIBRARY_DIRS} )




# Call the find_package_handle_standard_args() macro to set the _FOUND variable and print a success or failure message
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(hiredis  DEFAULT_MSG HIREDIS_LIBRARIES HIREDIS_INCLUDE_DIR)


# https://cmake.org/cmake/help/latest/command/mark_as_advanced.html?highlight=s
# Don't show vars in CMakeGUI unless the "show advanced" option is set
#mark_as_advanced(HIREDIS_INCLUDE_DIR HIREDIS_LIBRARIES )


if(HIREDIS_FOUND)
	message(STATUS "Hiredis found")
else()
	message(STATUS "Hiredis not found")
endif()

