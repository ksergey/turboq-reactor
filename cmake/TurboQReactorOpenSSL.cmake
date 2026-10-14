# OpenSSL.
#
# In:  turboq_reactor_SYSTEM_OPENSSL (bool), turboq_reactor_OPENSSL_DIR (path, optional)
# Out: the targets OpenSSL::SSL and OpenSSL::Crypto.
#
# OpenSSL is built from source with the project (jimmy-park/openssl-cmake, fetched via CPM: it
# downloads the pinned OpenSSL release, checks its SHA-256 and runs OpenSSL's own Configure and
# make; static libraries unless BUILD_SHARED_LIBS) unless turboq_reactor_SYSTEM_OPENSSL asks for
# the system one (find_package). Needs Perl 5 and make.
#
# Built with:
#   enable-ktls  kernel TLS (off by default in upstream OpenSSL 3; distributions turn it on)
#   --openssldir the system's OPENSSLDIR (asked from the openssl tool, /etc/ssl otherwise), so that
#                the default certificate store and openssl.cnf of the distribution are used:
#                SSL_CTX_set_default_verify_paths() then finds the system CA certificates.

set(TURBOQ_REACTOR_OPENSSL_CMAKE_VERSION 3.5.4) # the OpenSSL 3.5 LTS release it builds

if (turboq_reactor_SYSTEM_OPENSSL)
    find_package(OpenSSL 3.0 REQUIRED)
    message(STATUS "turboq-reactor: system OpenSSL ${OPENSSL_VERSION}")
    return()
endif()

if (NOT turboq_reactor_OPENSSL_DIR)
    set(_openssl_dir "/etc/ssl")
    find_program(_openssl_tool openssl)
    if (_openssl_tool)
        execute_process(
            COMMAND "${_openssl_tool}" version -d
            OUTPUT_VARIABLE _openssl_tool_output
            RESULT_VARIABLE _openssl_tool_result
            ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
        # OPENSSLDIR: "/usr/lib/ssl"
        if (_openssl_tool_result EQUAL 0 AND _openssl_tool_output MATCHES "OPENSSLDIR: \"([^\"]+)\"")
            set(_openssl_dir "${CMAKE_MATCH_1}")
        endif()
    endif()
    set(turboq_reactor_OPENSSL_DIR "${_openssl_dir}" CACHE PATH
        "OPENSSLDIR of the OpenSSL built with the project: where it looks for CA certificates and openssl.cnf")
endif()

CPMAddPackage(
    NAME openssl-cmake
    GITHUB_REPOSITORY jimmy-park/openssl-cmake
    GIT_TAG ${TURBOQ_REACTOR_OPENSSL_CMAKE_VERSION}
    OPTIONS
        "OPENSSL_CONFIGURE_OPTIONS enable-ktls\\\\;no-docs\\\\;--openssldir=${turboq_reactor_OPENSSL_DIR}")

message(STATUS "turboq-reactor: OpenSSL ${TURBOQ_REACTOR_OPENSSL_CMAKE_VERSION} built from source "
               "(kTLS, OPENSSLDIR ${turboq_reactor_OPENSSL_DIR})")
