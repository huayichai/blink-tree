cmake_minimum_required(VERSION 3.10)
project(BLinkTree)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -lpthread")

add_executable(bench bench.cpp)