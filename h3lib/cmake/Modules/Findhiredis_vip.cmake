#https://gitlab.kitware.com/cmake/community/-/wikis/doc/tutorials/How-To-Find-Libraries

# - Try to find Redis cluster client (HIREDIS-VIP)
# Once done this will define
#  HIREDISVIP_FOUND - System has Redis client
#  HIREDISVIP_INCLUDE_DIRS - The client's include directories
#  HIREDISVIP_LIBRARIES - The libraries needed to use the client
#  HIREDISVIP_DEFINITIONS - Compiler switches required for using the client


# Use pkg-config to detect include/library paths hiredis_vip
find_package(PkgConfig)
pkg_check_modules(PC_HIREDIS_VIP QUIET hiredis_vip)
set(HIREDIS_VIP_DEFINITIONS ${PC_HIREDIS_VIP_CFLAGS_OTHER})


# Dependencies use plural forms, the package itself uses the singular forms defined by find_path and find_library
find_path(HIREDIS_VIP_INCLUDE_DIR hiredis-vip/hiredis.h
          HINTS ${PC_HIREDIS_VIP_INCLUDEDIR} ${PC_HIREDIS_VIP_INCLUDE_DIRS}
          PATH_SUFFIXES include )
          
find_library(HIREDIS_VIP_LIBRARIES hiredis_vip
             HINTS ${PC_HIREDIS_VIP_LIBDIR} ${PC_HIREDIS_VIP_LIBRARY_DIRS} )
             
                        
          

# Call the find_package_handle_standard_args() macro to set the _FOUND variable and print a success or failure message
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(hiredis_vip  DEFAULT_MSG HIREDIS_VIP_LIBRARIES HIREDIS_VIP_INCLUDE_DIR)
                                  
 
# https://cmake.org/cmake/help/latest/command/mark_as_advanced.html?highlight=s
# Don't show vars in CMakeGUI unless the "show advanced" option is set                               
#mark_as_advanced(HIREDIS_VIP_INCLUDE_DIR HIREDIS_VIP_LIBRARIES )  


if(HIREDIS_VIP_FOUND)
	message(STATUS "HIREDIS_VIP found")
else()
	message(STATUS "HIREDIS_VIP not found")
endif()
                                
