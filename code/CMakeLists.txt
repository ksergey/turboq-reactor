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
target_include_directories(${TargetName}
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(${TargetName}
    PUBLIC turboq::turboq PkgConfig::liburing OpenSSL::SSL OpenSSL::Crypto)

if (turboq_reactor_SANITIZER)
    target_compile_options(${TargetName}
        PUBLIC -fsanitize=address,leak,undefined)
    target_link_options(${TargetName}
        PUBLIC -fsanitize=address,leak,undefined)
endif()

file(GLOB_RECURSE Sources "${CMAKE_CURRENT_SOURCE_DIR}/turboq/reactor/*.cpp")
file(GLOB_RECURSE Headers "${CMAKE_CURRENT_SOURCE_DIR}/turboq/reactor/*.h")
list(FILTER Headers EXCLUDE REGEX "TestBackend\\.h$")

if (turboq_reactor_BUILD_TESTS)
    TurboQReactorAddTestsFromSourceList(Sources
        PREFIX ${TargetName}
        COMPILE_OPTIONS -Wall -Wextra -g
        LIBS doctest::doctest_with_main ${TargetName})

    # Connection tests once more against the epoll backend (see TestBackend.h).
    set(EpollTestSources ${Sources})
    list(FILTER EpollTestSources INCLUDE REGEX "(Connection|Lifetime)_test\\.cpp$")
    TurboQReactorAddTestsFromSourceList(EpollTestSources
        PREFIX ${TargetName}-epoll
        COMPILE_OPTIONS -Wall -Wextra -g
        DEFINITIONS TURBOQ_REACTOR_TEST_EPOLL
        LIBS doctest::doctest_with_main ${TargetName})
endif()

TurboQReactorAddBenchmarksFromSourceList(Sources
    PREFIX ${TargetName}
    COMPILE_OPTIONS -Wall -Wextra -g
    LIBS benchmark::benchmark_main ${TargetName})

TurboQReactorExcludeTestsAndBenchmarksFromSourceList(Sources)

foreach(file ${Headers})
    target_sources(${TargetName} PUBLIC ${file})
endforeach()

foreach(file ${Sources})
    target_sources(${TargetName} PRIVATE ${file})
endforeach()

add_library(turboq::reactor ALIAS ${TargetName})
