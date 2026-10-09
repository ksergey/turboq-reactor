# io_uring backend and liburing.
#
# In:  turboq_reactor_IO_URING (AUTO, ON, OFF), turboq_reactor_SYSTEM_LIBURING (bool)
# Out: TURBOQ_REACTOR_IO_URING_ENABLED (ON/OFF) and, when ON, the target liburing::liburing.
#
# liburing is built from source (fetched via CPM, a pinned release) unless
# turboq_reactor_SYSTEM_LIBURING asks for the system one (pkg-config). AUTO leaves the io_uring
# backend out when liburing can't be had (system liburing missing or too old, or its configure
# failing); ON makes that an error.

set(TURBOQ_REACTOR_LIBURING_VERSION 2.15)
set(TURBOQ_REACTOR_LIBURING_MIN_SYSTEM_VERSION 2.5)

set(TURBOQ_REACTOR_IO_URING_ENABLED OFF)
set(_liburing_problem "")

if (turboq_reactor_IO_URING STREQUAL "OFF")
    set(_liburing_problem "disabled by turboq_reactor_IO_URING=OFF")

elseif (turboq_reactor_SYSTEM_LIBURING)
    find_package(PkgConfig)
    if (PkgConfig_FOUND)
        pkg_check_modules(liburing IMPORTED_TARGET GLOBAL liburing>=${TURBOQ_REACTOR_LIBURING_MIN_SYSTEM_VERSION})
    endif()
    if (liburing_FOUND)
        add_library(liburing::liburing ALIAS PkgConfig::liburing)
        set(TURBOQ_REACTOR_IO_URING_ENABLED ON)
        set(_liburing_from "system liburing ${liburing_VERSION}")
    else()
        set(_liburing_problem "system liburing>=${TURBOQ_REACTOR_LIBURING_MIN_SYSTEM_VERSION} not found (pkg-config)")
    endif()

else()
    CPMAddPackage(
        NAME liburing
        GITHUB_REPOSITORY axboe/liburing
        GIT_TAG liburing-${TURBOQ_REACTOR_LIBURING_VERSION}
        DOWNLOAD_ONLY YES)

    # liburing has no CMake build: run its configure (generates config-host.h and the compat
    # headers) on a copy inside the build tree, so the CPM source cache stays untouched, then
    # build the library from its sources. Configure runs again only when the release or the C
    # compiler changes.
    enable_language(C)
    set(_dir "${CMAKE_BINARY_DIR}/_deps/liburing-configured")
    set(_stamp "${TURBOQ_REACTOR_LIBURING_VERSION};${CMAKE_C_COMPILER}")
    set(_stamp_file "${_dir}/.turboq-reactor-stamp")
    set(_previous_stamp "")
    if (EXISTS "${_stamp_file}")
        file(READ "${_stamp_file}" _previous_stamp)
    endif()
    if (NOT _previous_stamp STREQUAL _stamp)
        file(REMOVE_RECURSE "${_dir}")
        file(COPY "${liburing_SOURCE_DIR}/" DESTINATION "${_dir}" PATTERN ".git" EXCLUDE)
        # --use-libc: link against libc like any other static library (the default nolibc build
        # needs freestanding flags).
        execute_process(
            COMMAND sh ./configure --use-libc "--cc=${CMAKE_C_COMPILER}"
            WORKING_DIRECTORY "${_dir}"
            RESULT_VARIABLE _result
            OUTPUT_VARIABLE _output
            ERROR_VARIABLE _output)
        if (_result EQUAL 0)
            file(WRITE "${_stamp_file}" "${_stamp}")
        else()
            set(_liburing_problem "liburing configure failed:\n${_output}")
        endif()
    endif()

    if (NOT _liburing_problem)
        add_library(turboq-reactor-liburing STATIC
            "${_dir}/src/setup.c"
            "${_dir}/src/queue.c"
            "${_dir}/src/register.c"
            "${_dir}/src/syscall.c"
            "${_dir}/src/version.c")
        # The flags of liburing's src/Makefile.
        target_compile_definitions(turboq-reactor-liburing
            PRIVATE _GNU_SOURCE _LARGEFILE_SOURCE _FILE_OFFSET_BITS=64 LIBURING_INTERNAL)
        target_compile_options(turboq-reactor-liburing
            PRIVATE -include "${_dir}/config-host.h" -Wno-unused-parameter)
        target_include_directories(turboq-reactor-liburing
            SYSTEM PUBLIC "${_dir}/src/include")
        set_target_properties(turboq-reactor-liburing
            PROPERTIES POSITION_INDEPENDENT_CODE ON)
        add_library(liburing::liburing ALIAS turboq-reactor-liburing)
        set(TURBOQ_REACTOR_IO_URING_ENABLED ON)
        set(_liburing_from "liburing ${TURBOQ_REACTOR_LIBURING_VERSION} built from source")
    endif()
endif()

if (TURBOQ_REACTOR_IO_URING_ENABLED)
    message(STATUS "turboq-reactor: io_uring backend ON (${_liburing_from})")
elseif (turboq_reactor_IO_URING STREQUAL "ON")
    message(FATAL_ERROR "turboq-reactor: io_uring backend requested (turboq_reactor_IO_URING=ON) but ${_liburing_problem}")
else()
    message(STATUS "turboq-reactor: io_uring backend OFF (${_liburing_problem}), epoll is the default backend")
endif()
