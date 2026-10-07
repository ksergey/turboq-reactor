CPMAddPackage("gh:jarro2783/cxxopts@3.3.1")

add_executable(binance_market_data binance_market_data.cpp)
target_compile_options(binance_market_data
    PRIVATE -Wall -Wextra -Wattributes -Wpedantic -Wstrict-aliasing -Wcast-align -g)
target_compile_definitions(binance_market_data
    PRIVATE -DCXXOPTS_NO_RTTI)
target_link_libraries(binance_market_data
    PRIVATE cxxopts::cxxopts turboq::reactor)
