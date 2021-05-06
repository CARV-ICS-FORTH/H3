#https://gitlab.kitware.com/cmake/community/-/wikis/doc/tutorials/How-To-Find-Libraries

# - Try to find Kreon RDMA
# Once done this will define
#  KREON_RDMA_FOUND - System has Kreon
#  KREON_RDMA_INCLUDE_DIRS - The Kreon include directories
#  KREON_RDMA_LIBRARIES - The libraries needed to use Kreon
#  KREON_RDMA_DEFINITIONS - Compiler switches required for using Kreon


# Use pkg-config to detect include/library paths  kreon_client kreon_rdma kreonr
find_package(PkgConfig)
pkg_check_modules(PC_KREON_RDMA QUIET kreonr)
set(KREON_RDMA_DEFINITIONS ${PC_KREON_RDMA_CFLAGS_OTHER})


# Dependencies use plural forms, the package itself uses the singular forms defined by find_path and find_library
find_path(KREON_RDMA_INCLUDE_DIR kreon/kreon_rdma_client.h
          HINTS ${PC_KREON_RDMA_INCLUDEDIR} ${PC_KREON_RDMA_INCLUDE_DIRS}
          PATH_SUFFIXES include )

find_library(KREON_RDMA_LIBRARY_CLIENT kreon_client
             HINTS ${PC_KREON_RDMA_LIBDIR} ${PC_KREON_RDMA_LIBRARY_DIRS} )

find_library(KREON_RDMA_LIBRARY_RDMA kreon_rdma
             HINTS ${PC_KREON_RDMA_LIBDIR} ${PC_KREON_RDMA_LIBRARY_DIRS} )

find_library(KREON_RDMA_LIBRARY kreonr
             HINTS ${PC_KREON_RDMA_LIBDIR} ${PC_KREON_RDMA_LIBRARY_DIRS} )

# Call the find_package_handle_standard_args() macro to set the _FOUND variable and print a success or failure message
include(FindPackageHandleStandardArgs)


find_package_handle_standard_args(kreonrdma  DEFAULT_MSG
                                  KREON_RDMA_LIBRARY KREON_RDMA_LIBRARY_CLIENT KREON_RDMA_LIBRARY_RDMA KREON_RDMA_INCLUDE_DIR)


# https://cmake.org/cmake/help/latest/command/mark_as_advanced.html?highlight=s
# Don't show vars in CMakeGUI unless the "show advanced" option is set
#mark_as_advanced(KREON_RDMA_INCLUDE_DIR KREON_RDMA_LIBRARY )


if(KREON_RDMA_FOUND)
	set(KREON_RDMA_LIBRARIES ${KREON_RDMA_LIBRARY} ${KREON_RDMA_LIBRARY_CLIENT} ${KREON_RDMA_LIBRARY_RDMA})
	set(KREON_RDMA_DEFINITIONS ${KREON_RDMA_INCLUDE_DIR})
	message(STATUS "Kreon RDMA found")
else()
	message(STATUS "Kreon RDMA not found")
endif()

