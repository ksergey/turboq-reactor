# Modules: one library per directory, sources under <module>/turboq/reactor (headers are included
# as <turboq/reactor/...> from every module). Connection templates are header-only: they are
# instantiated in the user's code for the backend of the reactor; the libraries hold the rest.
#
#   module      target                       contents                                depends on
#   core        turboq-reactor::core         Reactor, epoll backend, TCP, UDP,       turboq
#                                            addresses, errors
#   uring       turboq-reactor::uring        io_uring backend (the default backend   core, liburing
#                                            when linked)
#   ssl         turboq-reactor::ssl          TLS: kernel TLS, OpenSSL fallback       core, OpenSSL
#   websocket   turboq-reactor::websocket    WebSocket client (ws://, wss://)        ssl
#   xdp         turboq-reactor::xdp          AF_XDP receive                          core
#
# turboq::reactor (turboq-reactor) links every module that is built.

set(TURBOQ_REACTOR_MODULES "")

# turboq_reactor_add_module(<name> DEPENDS <targets...> [DEFINITIONS <public definitions...>])
function(turboq_reactor_add_module Name)
    cmake_parse_arguments(ARG "" "" "DEPENDS;DEFINITIONS" ${ARGN})
    set(Target turboq-reactor-${Name})
    set(Root ${CMAKE_CURRENT_SOURCE_DIR}/${Name})

    file(GLOB_RECURSE Sources CONFIGURE_DEPENDS "${Root}/turboq/reactor/*.cpp")
    file(GLOB_RECURSE Headers CONFIGURE_DEPENDS "${Root}/turboq/reactor/*.h")
    list(FILTER Headers EXCLUDE REGEX "TestBackend\\.h$")
    set(Tests ${Sources})
    list(FILTER Tests INCLUDE REGEX "_test\\.cpp$")
    TurboQExcludeTestsAndBenchmarksFromSourceList(Sources)

    add_library(${Target} STATIC ${Sources})
    target_sources(${Target} PRIVATE ${Headers})
    target_compile_features(${Target} PUBLIC cxx_std_23)
    set_target_properties(${Target}
        PROPERTIES
            CXX_STANDARD_REQUIRED ON
            CXX_EXTENSIONS OFF
            POSITION_INDEPENDENT_CODE ON)
    target_compile_options(${Target}
        PUBLIC -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g -fno-rtti)
    if (turboq_reactor_SANITIZER)
        target_compile_options(${Target} PUBLIC -fsanitize=address,leak,undefined)
        target_link_options(${Target} PUBLIC -fsanitize=address,leak,undefined)
    endif()
    target_include_directories(${Target} PUBLIC ${Root})
    target_link_libraries(${Target} PUBLIC ${ARG_DEPENDS})
    target_compile_definitions(${Target} PUBLIC ${ARG_DEFINITIONS})
    add_library(turboq-reactor::${Name} ALIAS ${Target})

    set(TURBOQ_REACTOR_MODULES ${TURBOQ_REACTOR_MODULES} ${Target} PARENT_SCOPE)
    set(TURBOQ_REACTOR_TESTS ${TURBOQ_REACTOR_TESTS} ${Tests} PARENT_SCOPE)
endfunction()

turboq_reactor_add_module(core DEPENDS turboq::turboq)

if (TURBOQ_REACTOR_IO_URING_ENABLED)
    turboq_reactor_add_module(uring
        DEPENDS turboq-reactor::core liburing::liburing
        DEFINITIONS TURBOQ_REACTOR_IO_URING=1)
endif()

if (turboq_reactor_SSL)
    turboq_reactor_add_module(ssl DEPENDS turboq-reactor::core OpenSSL::SSL OpenSSL::Crypto)
    if (turboq_reactor_WEBSOCKET)
        turboq_reactor_add_module(websocket DEPENDS turboq-reactor::ssl)
    endif()
endif()

if (turboq_reactor_XDP)
    turboq_reactor_add_module(xdp DEPENDS turboq-reactor::core)
endif()

add_library(turboq-reactor INTERFACE)
target_link_libraries(turboq-reactor INTERFACE ${TURBOQ_REACTOR_MODULES})
add_library(turboq::reactor ALIAS turboq-reactor)
message(STATUS "turboq-reactor: modules ${TURBOQ_REACTOR_MODULES}")

if (turboq_reactor_BUILD_TESTS)
    # Tests link every module that is built; a test touching another module checks for its headers
    # (__has_include) and skips that part when the module is off.
    TurboQAddTestsFromSourceList(TURBOQ_REACTOR_TESTS
        PREFIX turboq-reactor
        OPTIONS -Wall -Wextra -g
        LIBS doctest::doctest_with_main turboq-reactor)

    # Connection tests once more against the epoll backend (see TestBackend.h). Without io_uring
    # the tests above already run on epoll.
    if (TURBOQ_REACTOR_IO_URING_ENABLED)
        set(EpollTests ${TURBOQ_REACTOR_TESTS})
        list(FILTER EpollTests INCLUDE REGEX "(Connection|Lifetime)_test\\.cpp$")
        TurboQAddTestsFromSourceList(EpollTests
            PREFIX turboq-reactor-epoll
            OPTIONS -Wall -Wextra -g
            DEFINITIONS TURBOQ_REACTOR_TEST_EPOLL
            LIBS doctest::doctest_with_main turboq-reactor)
    endif()
endif()
