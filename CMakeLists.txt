cmake_minimum_required(VERSION 3.24)

project(turboq-reactor CXX)

option(turboq_reactor_SANITIZER "Build with sanitizer" OFF)
option(turboq_reactor_BUILD_TESTS "Build tests (*_test.cpp files, linked against doctest)" ON)
option(turboq_reactor_TOOLS "Build tools (requires cxxopts, fetched via CPM)" ON)
option(turboq_reactor_EXAMPLES "Build examples (requires cxxopts, fetched via CPM)" ON)
option(turboq_reactor_WERROR "Treat warnings as errors in this project's code (CI)" OFF)

include(cmake/TurboQReactorHelpers.cmake)
include(cmake/GetCPM.cmake)

find_package(PkgConfig REQUIRED)
pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing>=2.5)
find_package(OpenSSL 3.0 REQUIRED)

CPMAddPackage(
    NAME turboq
    GITHUB_REPOSITORY ksergey/turboq
    GIT_TAG 0.2.0
    OPTIONS "turboq_BUILD_TESTS OFF" "turboq_TOOLS OFF" "turboq_PYTHON OFF")

if (turboq_reactor_BUILD_TESTS)
    CPMAddPackage("gh:doctest/doctest@2.5.2")
    enable_testing()
endif()

# After the dependencies: only this project's targets are affected.
if (turboq_reactor_WERROR)
    add_compile_options(-Werror)
endif()

add_subdirectory(code)

if (turboq_reactor_TOOLS)
    add_subdirectory(tools)
endif()

if (turboq_reactor_EXAMPLES)
    add_subdirectory(examples)
endif()
