cmake_minimum_required(VERSION 3.19)
project(map_reduce)

set(CMAKE_CXX_STANDARD 11)

find_package(MPI REQUIRED)

add_executable(map_reduce src/main.cc src/Scheduler.cc src/include/Scheduler.h src/Worker.cc src/include/Worker.h src/ThreadPool.cc src/include/ThreadPool.h)

target_link_libraries(map_reduce PUBLIC MPI::MPI_CXX)
