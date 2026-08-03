# CMake generated Testfile for 
# Source directory: /workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5
# Build directory: /workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(leveldb_tests "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/leveldb_tests")
set_tests_properties(leveldb_tests PROPERTIES  _BACKTRACE_TRIPLES "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;395;add_test;/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;0;")
add_test(c_test "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/c_test")
set_tests_properties(c_test PROPERTIES  _BACKTRACE_TRIPLES "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;422;add_test;/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;425;leveldb_test;/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;0;")
add_test(env_posix_test "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/build_baseline/env_posix_test")
set_tests_properties(env_posix_test PROPERTIES  _BACKTRACE_TRIPLES "/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;422;add_test;/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;433;leveldb_test;/workspace/a0141180-ba0e-411c-a71d-64fed67ca5ec/sessions/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/CMakeLists.txt;0;")
subdirs("third_party/googletest")
subdirs("third_party/benchmark")
