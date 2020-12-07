cmake_minimum_required(VERSION 3.15)
project(p4a C)

set(CMAKE_C_STANDARD 99)

add_executable(p4a wordcount.c mapreduce.c mapreduce.h)