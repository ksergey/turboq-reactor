set(TargetName turboq-reactor)

add_library(${TargetName} STATIC)
target_compile_features(${TargetName}
    PUBLIC cxx_std_23)
set_target_properties(${TargetName}
    PROPERTIES
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        POSITION_INDEPENDENT_CODE ON)
target_compile_options(${TargetName}
    PUBLIC -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g -fno-rtti)

# Build configuration for the headers: turboq/reactor/Config.h.
if (TURBOQ_REACTOR_IO_URING_ENABLED)
    set(TURBOQ_REACTOR_IO_URING 1)
else()
    set(TURBOQ_REACTOR_IO_URING 0)
endif()
configure_file(turboq/reactor/Config.h.in ${CMAKE_CURRENT_BINARY_DIR}/generated/turboq/reactor/Config.h @ONLY)

target_include_directories(${TargetName}
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/generated)
target_link_libraries(${TargetName}
    PUBLIC turboq::turboq OpenSSL::SSL OpenSSL::Crypto)
if (TURBOQ_REACTOR_IO_URING_ENABLED)
    target_link_libraries(${TargetName}
        PUBLIC liburing::liburing)
endif()

if (turboq_reactor_SANITIZER)
    target_compile_options(${TargetName}
        PUBLIC -fsanitize=address,leak,undefined)
    target_link_options(${TargetName}
        PUBLIC -fsanitize=address,leak,undefined)
endif()

file(GLOB_RECURSE Sources "${CMAKE_CURRENT_SOURCE_DIR}/turboq/reactor/*.cpp")
file(GLOB_RECURSE Headers "${CMAKE_CURRENT_SOURCE_DIR}/turboq/reactor/*.h")
list(FILTER Headers EXCLUDE REGEX "TestBackend\\.h$")
if (NOT TURBOQ_REACTOR_IO_URING_ENABLED)
    list(FILTER Sources EXCLUDE REGEX "IoUringBackend\\.cpp$")
    list(FILTER Headers EXCLUDE REGEX "IoUringBackend\\.h$")
endif()

if (turboq_reactor_BUILD_TESTS)
    TurboQAddTestsFromSourceList(Sources
        PREFIX ${TargetName}
        COMPILE_OPTIONS -Wall -Wextra -g
        LIBS doctest::doctest_with_main ${TargetName})

    # Connection tests once more against the epoll backend (see TestBackend.h). Without io_uring
    # the tests above already run on epoll.
    if (TURBOQ_REACTOR_IO_URING_ENABLED)
        set(EpollTestSources ${Sources})
        list(FILTER EpollTestSources INCLUDE REGEX "(Connection|Lifetime)_test\\.cpp$")
        TurboQAddTestsFromSourceList(EpollTestSources
            PREFIX ${TargetName}-epoll
            COMPILE_OPTIONS -Wall -Wextra -g
            DEFINITIONS TURBOQ_REACTOR_TEST_EPOLL
            LIBS doctest::doctest_with_main ${TargetName})
    endif()
endif()

TurboQAddBenchmarksFromSourceList(Sources
    PREFIX ${TargetName}
    COMPILE_OPTIONS -Wall -Wextra -g
    LIBS benchmark::benchmark_main ${TargetName})

TurboQExcludeTestsAndBenchmarksFromSourceList(Sources)

foreach(file ${Headers})
    target_sources(${TargetName} PUBLIC ${file})
endforeach()

foreach(file ${Sources})
    target_sources(${TargetName} PRIVATE ${file})
endforeach()

add_library(turboq::reactor ALIAS ${TargetName})
