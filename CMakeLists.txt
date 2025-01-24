cmake_minimum_required(VERSION 3.10)
project(zmq_wordcount C)

set(CMAKE_C_STANDARD 11)

find_package(Threads REQUIRED)
set(CPACK_SOURCE_GENERATOR "TGZ")

# Явно добавить папки, где лежат заголовки и либы
include_directories(/opt/homebrew/include)
link_directories(/opt/homebrew/lib)

# Собираем Distributor
add_executable(zmq_distributor zmq_distributor.c)
target_link_libraries(zmq_distributor PRIVATE zmq Threads::Threads)

# Собираем Worker
add_executable(zmq_worker zmq_worker.c)
target_link_libraries(zmq_worker PRIVATE zmq Threads::Threads)
