# Install script for directory: /workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Release")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/libleveldb.a")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/leveldb" TYPE FILE FILES
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/c.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/cache.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/comparator.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/db.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/dumpfile.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/env.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/export.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/filter_policy.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/iterator.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/options.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/slice.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/status.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/table_builder.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/table.h"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/include/leveldb/write_batch.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/leveldb/leveldbTargets.cmake")
    file(DIFFERENT EXPORT_FILE_CHANGED FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/leveldb/leveldbTargets.cmake"
         "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/CMakeFiles/Export/lib/cmake/leveldb/leveldbTargets.cmake")
    if(EXPORT_FILE_CHANGED)
      file(GLOB OLD_CONFIG_FILES "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/leveldb/leveldbTargets-*.cmake")
      if(OLD_CONFIG_FILES)
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/leveldb/leveldbTargets.cmake\" will be replaced.  Removing files [${OLD_CONFIG_FILES}].")
        file(REMOVE ${OLD_CONFIG_FILES})
      endif()
    endif()
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/leveldb" TYPE FILE FILES "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/CMakeFiles/Export/lib/cmake/leveldb/leveldbTargets.cmake")
  if("${CMAKE_INSTALL_CONFIG_NAME}" MATCHES "^([Rr][Ee][Ll][Ee][Aa][Ss][Ee])$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/leveldb" TYPE FILE FILES "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/CMakeFiles/Export/lib/cmake/leveldb/leveldbTargets-release.cmake")
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/leveldb" TYPE FILE FILES
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/cmake/leveldbConfig.cmake"
    "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/cmake/leveldbConfigVersion.cmake"
    )
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/third_party/googletest/cmake_install.cmake")
  include("/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/third_party/benchmark/cmake_install.cmake")

endif()

if(CMAKE_INSTALL_COMPONENT)
  set(CMAKE_INSTALL_MANIFEST "install_manifest_${CMAKE_INSTALL_COMPONENT}.txt")
else()
  set(CMAKE_INSTALL_MANIFEST "install_manifest.txt")
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
file(WRITE "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/${CMAKE_INSTALL_MANIFEST}"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
