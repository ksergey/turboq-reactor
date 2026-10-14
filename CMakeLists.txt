cmake_minimum_required(VERSION 3.25)

project(turboq-reactor CXX)

option(turboq_reactor_SANITIZER "Build with sanitizer" OFF)
option(turboq_reactor_BUILD_TESTS "Build tests (*_test.cpp files, linked against doctest)" ON)
option(turboq_reactor_TOOLS "Build tools (requires cxxopts, fetched via CPM)" ON)
option(turboq_reactor_EXAMPLES "Build examples (requires cxxopts, fetched via CPM)" ON)
option(turboq_reactor_WERROR "Treat warnings as errors in this project's code (CI)" OFF)
set(turboq_reactor_IO_URING "AUTO" CACHE STRING
    "io_uring backend: AUTO (built when liburing is available), ON (required) or OFF (epoll only)")
set_property(CACHE turboq_reactor_IO_URING PROPERTY STRINGS AUTO ON OFF)
option(turboq_reactor_SYSTEM_LIBURING "Use the system liburing (pkg-config) instead of building it via CPM" OFF)
option(turboq_reactor_SYSTEM_OPENSSL "Use the system OpenSSL (find_package) instead of building it via CPM" OFF)
option(turboq_reactor_SSL "Build turboq-reactor::ssl (TLS; needs OpenSSL)" ON)
option(turboq_reactor_WEBSOCKET "Build turboq-reactor::websocket (needs turboq_reactor_SSL)" ON)
option(turboq_reactor_XDP "Build turboq-reactor::xdp (AF_XDP receive)" ON)

include(cmake/GetCPM.cmake)

include(cmake/TurboQReactorLiburing.cmake)
if (turboq_reactor_SSL)
    include(cmake/TurboQReactorOpenSSL.cmake)
elseif (turboq_reactor_WEBSOCKET)
    message(STATUS "turboq-reactor: websocket needs ssl (turboq_reactor_SSL): not built")
endif()

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
