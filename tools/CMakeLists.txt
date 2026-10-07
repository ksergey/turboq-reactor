CPMAddPackage("gh:jarro2783/cxxopts@3.3.1")

add_executable(tcp_pingpong tcp_pingpong.cpp)
target_compile_options(tcp_pingpong
    PRIVATE -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g)
target_compile_definitions(tcp_pingpong
    PRIVATE -DCXXOPTS_NO_RTTI)
target_link_libraries(tcp_pingpong
    PRIVATE cxxopts::cxxopts turboq::reactor)

add_executable(udp_listen udp_listen.cpp)
target_compile_options(udp_listen
    PRIVATE -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g)
target_compile_definitions(udp_listen
    PRIVATE -DCXXOPTS_NO_RTTI)
target_link_libraries(udp_listen
    PRIVATE cxxopts::cxxopts turboq::reactor)
