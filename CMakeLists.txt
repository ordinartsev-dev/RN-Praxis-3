cmake_minimum_required(VERSION 3.5)
project(RN-Praxis)
set(CMAKE_C_STANDARD 11)

find_library(ZeroMQ zmq REQUIRED)

add_executable(zmq_distributor zmq_distributor.c)
target_compile_options(zmq_distributor PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(zmq_distributor PRIVATE zmq pthread)

add_executable(zmq_worker zmq_worker.c)
target_compile_options(zmq_worker PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(zmq_worker PRIVATE zmq pthread)

# Packaging
set(CPACK_SOURCE_GENERATOR "TGZ")
set(CPACK_SOURCE_IGNORE_FILES ${CMAKE_BINARY_DIR} /\\..*$ \\.pdf$ /build/)
set(CPACK_VERBATIM_VARIABLES YES)
include(CPack)