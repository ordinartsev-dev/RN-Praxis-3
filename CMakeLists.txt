cmake_minimum_required(VERSION 3.5)
project(RN-Praxis)
set(CMAKE_C_STANDARD 11)

# Найти библиотеку zmq (libzmq)
find_library(ZMQ_LIB zmq REQUIRED HINTS /usr/local/lib /opt/homebrew/lib)

# Если нужно, вы можете найти pthread по аналогии или полагаться на встроенную поддержку.
# find_package(Threads REQUIRED)

# Добавить включаемые пути, где лежит zmq.h
# Для Intel macOS (обычно /usr/local/...):
#include_directories(/usr/local/include)

# Для Apple Silicon (обычно /opt/homebrew/...):
include_directories(/opt/homebrew/include)

# Теперь создаём цели
add_executable(zmq_distributor zmq_distributor.c)
target_compile_options(zmq_distributor PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(zmq_distributor PRIVATE ${ZMQ_LIB} pthread)

add_executable(zmq_worker zmq_worker.c)
target_compile_options(zmq_worker PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(zmq_worker PRIVATE ${ZMQ_LIB} pthread)

# Packaging (опционально)
set(CPACK_SOURCE_GENERATOR "TGZ")
set(CPACK_SOURCE_IGNORE_FILES ${CMAKE_BINARY_DIR} /\\..*$ \\.pdf$ /build/)
set(CPACK_VERBATIM_VARIABLES YES)
include(CPack)
