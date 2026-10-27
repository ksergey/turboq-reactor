# turboq::reactor: header-only, depends on turboq only.
#
# Parts that need other libraries are only compiled when used, and then need that library linked
# by the user:
#   IoUringBackend (IoUringBackend.h)               liburing
#   TLSConnection, WebsocketConnection               OpenSSL (OpenSSL::SSL, OpenSSL::Crypto)
#
# Reactor<>, TCPConnection<>, ... use EpollBackend unless TURBOQ_REACTOR_IO_URING=1 is defined (for
# the whole program), which makes io_uring the default backend.

add_library(turboq-reactor INTERFACE)
add_library(turboq::reactor ALIAS turboq-reactor)

target_compile_features(turboq-reactor
    INTERFACE cxx_std_23)
target_compile_options(turboq-reactor
    INTERFACE -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g -fno-rtti)
if (turboq_reactor_SANITIZER)
    target_compile_options(turboq-reactor
        INTERFACE -fsanitize=address,leak,undefined)
    target_link_options(turboq-reactor
        INTERFACE -fsanitize=address,leak,undefined)
endif()
target_include_directories(turboq-reactor
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turboq-reactor
    INTERFACE turboq::turboq)

if (turboq_reactor_BUILD_TESTS)
    file(GLOB Tests CONFIGURE_DEPENDS turboq-reactor/*_test.cpp)
    if (NOT TARGET OpenSSL::SSL)
        # TLS and WebSocket need OpenSSL; other tests skip their TLS/WebSocket parts.
        list(FILTER Tests EXCLUDE REGEX "(TLS|Websocket)Connection_test\\.cpp$")
    endif()

    TurboQAddTestsFromSourceList(Tests
        PREFIX turboq-reactor
        LIBS doctest::doctest_with_main turboq-reactor-dev)

    # Connection tests once more against the epoll backend (see TestBackend.h). Without io_uring
    # the tests above already run on epoll.
    if (TURBOQ_REACTOR_IO_URING_ENABLED)
        set(EpollTests ${Tests})
        list(FILTER EpollTests INCLUDE REGEX "(Connection|Lifetime)_test\\.cpp$")
        TurboQAddTestsFromSourceList(EpollTests
            PREFIX turboq-reactor-epoll
            DEFINITIONS TURBOQ_REACTOR_TEST_EPOLL
            LIBS doctest::doctest_with_main turboq-reactor-dev)
    endif()
endif()
