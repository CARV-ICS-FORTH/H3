#https://gitlab.kitware.com/cmake/community/-/wikis/doc/tutorials/How-To-Find-Libraries

# - Try to find Kreon
# Once done this will define
#  KREON_FOUND - System has Kreon
#  KREON_INCLUDE_DIRS - The Kreon include directories
#  KREON_LIBRARIES - The libraries needed to use Kreon
#  KREON_DEFINITIONS - Compiler switches required for using Kreon


# Use pkg-config to detect include/library paths  kreon_client kreon_rdma kreonr
find_package(PkgConfig)
pkg_check_modules(PC_KREON QUIET kreon)
set(KREON_DEFINITIONS ${PC_KREON_CFLAGS_OTHER})


# Dependencies use plural forms, the package itself uses the singular forms defined by find_path and find_library
find_path(KREON_INCLUDE_DIR kreon.h
          HINTS ${PC_KREON_INCLUDEDIR} ${PC_KREON_INCLUDE_DIRS}
          PATH_SUFFIXES include )

find_library(KREON_LIBRARY kreon
             HINTS ${PC_KREON_LIBDIR} ${PC_KREON_LIBRARY_DIRS} )

# Call the find_package_handle_standard_args() macro to set the _FOUND variable and print a success or failure message
include(FindPackageHandleStandardArgs)


find_package_handle_standard_args(kreon  DEFAULT_MSG
                                  KREON_LIBRARY KREON_INCLUDE_DIR)


# https://cmake.org/cmake/help/latest/command/mark_as_advanced.html?highlight=s
# Don't show vars in CMakeGUI unless the "show advanced" option is set
#mark_as_advanced(KREON_INCLUDE_DIR KREON_LIBRARY )


if(KREON_FOUND)
	set(KREON_LIBRARIES ${KREON_LIBRARY})
	set(KREON_DEFINITIONS ${KREON_INCLUDE_DIR})
	message(STATUS "Kreon found")
else()
	message(STATUS "Kreon not found")
endif()

