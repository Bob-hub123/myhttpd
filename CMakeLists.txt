# 注意：cmake_minimum_required 必须写在 project() 之前
cmake_minimum_required(VERSION 3.16)

project(myhttpd LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(myhttpd
        src/main.cpp
)

# GCC/Clang 下打开常用告警；MSVC 不认 -Wall -Wextra，故加编译器判断
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(myhttpd PRIVATE -Wall -Wextra)
endif()
