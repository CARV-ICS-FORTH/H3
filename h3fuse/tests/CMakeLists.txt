cmake_minimum_required(VERSION 3.10)

# Test 1
add_executable(test_syscalls test_syscalls.c)
add_test(syscalls test_syscalls)

