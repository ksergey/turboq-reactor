CPMAddPackage("gh:jarro2783/cxxopts@3.3.1")

add_executable(binance_market_data binance_market_data.cpp)
target_compile_options(binance_market_data
    PRIVATE -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g)
target_compile_definitions(binance_market_data
    PRIVATE -DCXXOPTS_NO_RTTI)
target_link_libraries(binance_market_data
    PRIVATE cxxopts::cxxopts turboq::reactor)

add_executable(multicast_feeds multicast_feeds.cpp)
target_compile_options(multicast_feeds
    PRIVATE -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g)
target_compile_definitions(multicast_feeds
    PRIVATE -DCXXOPTS_NO_RTTI)
target_link_libraries(multicast_feeds
    PRIVATE cxxopts::cxxopts turboq::reactor)

add_executable(xdp_feeds xdp_feeds.cpp)
target_compile_options(xdp_feeds
    PRIVATE -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g)
target_compile_definitions(xdp_feeds
    PRIVATE -DCXXOPTS_NO_RTTI)
target_link_libraries(xdp_feeds
    PRIVATE cxxopts::cxxopts turboq::reactor)
