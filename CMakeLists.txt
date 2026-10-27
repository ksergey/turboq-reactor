cmake_minimum_required(VERSION 3.24)

project(turboq-reactor CXX)

option(turboq_reactor_SANITIZER "Build with sanitizer" OFF)
option(turboq_reactor_BUILD_TESTS "Build tests (*_test.cpp files, linked against doctest)" ${PROJECT_IS_TOP_LEVEL})
option(turboq_reactor_TOOLS "Build tools (requires cxxopts, fetched via CPM)" ${PROJECT_IS_TOP_LEVEL})
option(turboq_reactor_EXAMPLES "Build examples (requires cxxopts, fetched via CPM)" ${PROJECT_IS_TOP_LEVEL})
# The library is header-only and needs turboq only. Tests, tools and examples also use io_uring
# (liburing) and TLS (OpenSSL) when available; these options only apply to them.
set(turboq_reactor_IO_URING "AUTO" CACHE STRING
    "io_uring in tests/tools/examples: AUTO (when liburing is available), ON (required) or OFF")
set_property(CACHE turboq_reactor_IO_URING PROPERTY STRINGS AUTO ON OFF)
option(turboq_reactor_SYSTEM_LIBURING "Use the system liburing (pkg-config) instead of building it via CPM" OFF)

include(cmake/GetCPM.cmake)

# Minimum build environment (see README.md "Requirements"): Linux kernel headers 6.1+ and
# glibc 2.35+ are needed at compile time in translation units that compile the header-only
# library (epoll_pwait2); they are not checked at configure time.

set(TURBOQ_REACTOR_DEV OFF)
if (turboq_reactor_BUILD_TESTS OR turboq_reactor_TOOLS OR turboq_reactor_EXAMPLES)
    set(TURBOQ_REACTOR_DEV ON)
    include(cmake/TurboQReactorLiburing.cmake)
    find_package(OpenSSL 3.0)
    if (NOT OpenSSL_FOUND)
        message(STATUS "turboq-reactor: OpenSSL not found: TLS and WebSocket tests and examples are not built")
    endif()
endif()

CPMAddPackage(
    NAME turboq
    GITHUB_REPOSITORY ksergey/turboq
    GIT_TAG master
    OPTIONS "turboq_BUILD_TESTS OFF" "turboq_TOOLS OFF" "turboq_PYTHON OFF")

if (turboq_reactor_BUILD_TESTS)
    CPMAddPackage("gh:doctest/doctest@2.5.2")
    enable_testing()
endif()

add_subdirectory(code)

# What the tests, tools and examples of this project link: the library plus liburing and OpenSSL
# when available (TURBOQ_REACTOR_IO_URING=1 makes io_uring the default backend,
# TURBOQ_REACTOR_HAS_OPENSSL=1 enables the TLS / WebSocket parts).
if (TURBOQ_REACTOR_DEV)
    add_library(turboq-reactor-dev INTERFACE)
    target_link_libraries(turboq-reactor-dev INTERFACE turboq::reactor)
    if (TURBOQ_REACTOR_IO_URING_ENABLED)
        target_link_libraries(turboq-reactor-dev INTERFACE liburing::liburing)
        target_compile_definitions(turboq-reactor-dev INTERFACE TURBOQ_REACTOR_IO_URING=1)
    endif()
    if (OpenSSL_FOUND)
        target_link_libraries(turboq-reactor-dev INTERFACE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(turboq-reactor-dev INTERFACE TURBOQ_REACTOR_HAS_OPENSSL=1)
    endif()
endif()

if (turboq_reactor_TOOLS)
    add_subdirectory(tools)
endif()

if (turboq_reactor_EXAMPLES)
    add_subdirectory(examples)
endif()
