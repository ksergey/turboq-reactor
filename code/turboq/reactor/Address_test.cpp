// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <net/if.h>
#include <netinet/in.h>

#include <format>
#include <ostream>
#include <string>
#include <unordered_set>

#include <doctest/doctest.h>

#include "Address.h"
#include "Error.h"

namespace turboq::reactor::testing {

TEST_SUITE("Address") {

    TEST_CASE("IPv4Address: parse and print") {
        for (char const* text : {"0.0.0.0", "127.0.0.1", "192.168.1.254", "239.255.10.1", "255.255.255.255"}) {
            CAPTURE(text);
            auto const address = IPv4Address::parse(text);
            REQUIRE(address);
            REQUIRE_EQ(toString(*address), text);
        }
        for (char const* text : {"", "1.2.3", "1.2.3.4.5", "256.0.0.1", "1.2.3.4 ", " 1.2.3.4", "01.2.3.4x", "::1",
                 "localhost", "1.2.3.4:80"}) {
            CAPTURE(text);
            auto const address = IPv4Address::parse(text);
            REQUIRE_FALSE(address);
            REQUIRE_EQ(address.error(), makeErrorCode(Error::InvalidAddress));
        }
        REQUIRE_FALSE(IPv4Address::parse(std::string_view{"1.2.3.4\0", 8}));
    }

    TEST_CASE("IPv4Address: values and predicates") {
        constexpr IPv4Address a{192, 168, 0, 1};
        static_assert(a.toUint() == 0xC0A80001);
        static_assert(IPv4Address{0xC0A80001u} == a);
        static_assert(IPv4Address::loopback().isLoopback());
        static_assert(IPv4Address{127, 1, 2, 3}.isLoopback());
        static_assert(IPv4Address{239, 1, 1, 1}.isMulticast());
        static_assert(IPv4Address{224, 0, 0, 1}.isMulticast());
        static_assert(!IPv4Address{240, 0, 0, 1}.isMulticast());
        static_assert(IPv4Address{}.isUnspecified());
        static_assert(IPv4Address{1, 2, 3, 4} < IPv4Address{1, 2, 3, 5});
        REQUIRE_EQ(a.bytes()[0], 192);
    }

    TEST_CASE("IPv6Address: parse and print") {
        for (char const* text : {"::", "::1", "2001:db8::1", "fe80::1:2:3:4", "ff02::1", "::ffff:10.0.0.1",
                 "2001:db8:85a3::8a2e:370:7334"}) {
            CAPTURE(text);
            auto const address = IPv6Address::parse(text);
            REQUIRE(address);
            REQUIRE_EQ(toString(*address), text);
        }
        // Not canonical in, canonical out.
        REQUIRE_EQ(toString(*IPv6Address::parse("2001:0DB8:0000:0000:0000:0000:0000:0001")), "2001:db8::1");

        for (char const* text : {"", ":::", "1:2:3:4:5:6:7:8:9", "2001:db8::g", "127.0.0.1", "[::1]", "::1%",
                 "fe80::1%no-such-interface0"}) {
            CAPTURE(text);
            REQUIRE_FALSE(IPv6Address::parse(text));
        }
    }

    TEST_CASE("IPv6Address: scope id") {
        auto const numeric = IPv6Address::parse("fe80::1%42");
        REQUIRE(numeric);
        REQUIRE_EQ(numeric->scopeId(), 42);
        REQUIRE(numeric->isLinkLocal());
        REQUIRE_EQ(toString(*numeric), "fe80::1%42"); // no interface 42: printed as a number

        auto const lo = ::if_nametoindex("lo");
        if (lo != 0) {
            auto const named = IPv6Address::parse("fe80::1%lo");
            REQUIRE(named);
            REQUIRE_EQ(named->scopeId(), lo);
            REQUIRE_EQ(toString(*named), "fe80::1%lo");
        }
        REQUIRE_NE(*IPv6Address::parse("fe80::1%1"), *IPv6Address::parse("fe80::1%2"));
    }

    TEST_CASE("IPv6Address: predicates and v4-mapped") {
        static_assert(IPv6Address::loopback().isLoopback());
        static_assert(IPv6Address{}.isUnspecified());
        static_assert(!IPv6Address::loopback().isUnspecified());
        constexpr auto mapped = IPv6Address::v4Mapped(IPv4Address{10, 0, 0, 1});
        static_assert(mapped.isV4Mapped());
        static_assert(mapped.toV4() == IPv4Address{10, 0, 0, 1});
        static_assert(!IPv6Address::loopback().toV4());
        REQUIRE(IPv6Address::parse("ff05::2")->isMulticast());
        REQUIRE_FALSE(IPv6Address::parse("fe80::1")->isMulticast());
        REQUIRE_EQ(toString(mapped), "::ffff:10.0.0.1");
    }

    TEST_CASE("IPAddress") {
        constexpr IPAddress defaultAddress;
        static_assert(defaultAddress.isV4() && defaultAddress.isUnspecified());
        static_assert(IPAddress{IPv6Address::loopback()}.family() == AF_INET6);

        auto const v4 = IPAddress::parse("10.1.2.3");
        REQUIRE(v4);
        REQUIRE(v4->isV4());
        REQUIRE_EQ(v4->v4(), IPv4Address{10, 1, 2, 3});
        REQUIRE_EQ(v4->family(), AF_INET);

        auto const v6 = IPAddress::parse("2001:db8::7");
        REQUIRE(v6);
        REQUIRE(v6->isV6());
        REQUIRE_EQ(toString(*v6), "2001:db8::7");
        REQUIRE_NE(*v4, *v6);

        REQUIRE(IPAddress::parse("239.1.1.1")->isMulticast());
        REQUIRE(IPAddress::parse("::1")->isLoopback());
        REQUIRE_FALSE(IPAddress::parse("example.com"));
        REQUIRE_FALSE(IPAddress::parse(""));
    }

    TEST_CASE("Endpoint: parse and print") {
        for (char const* text :
            {"127.0.0.1:80", "0.0.0.0:0", "239.1.1.1:65535", "[::1]:443", "[2001:db8::1]:9443", "[fe80::1%42]:5001"}) {
            CAPTURE(text);
            auto const endpoint = Endpoint::parse(text);
            REQUIRE(endpoint);
            REQUIRE_EQ(toString(*endpoint), text);
        }
        auto const v6 = Endpoint::parse("[::1]:8080");
        REQUIRE(v6);
        REQUIRE_EQ(v6->address, IPAddress{IPv6Address::loopback()});
        REQUIRE_EQ(v6->port, 8080);

        for (char const* text : {"", "127.0.0.1", "127.0.0.1:", ":80", "127.0.0.1:65536", "127.0.0.1:-1",
                 "127.0.0.1:80x", "::1:80", "[::1]", "[::1]80", "[127.0.0.1]:80", "localhost:80", "[::1:80"}) {
            CAPTURE(text);
            auto const endpoint = Endpoint::parse(text);
            REQUIRE_FALSE(endpoint);
            REQUIRE_EQ(endpoint.error(), makeErrorCode(Error::InvalidAddress));
        }
    }

    TEST_CASE("Endpoint: sockaddr round trip") {
        for (char const* text : {"192.0.2.33:1234", "[2001:db8::42]:4321", "[fe80::1%7]:1"}) {
            CAPTURE(text);
            auto const endpoint = *Endpoint::parse(text);
            sockaddr_storage storage;
            auto const length = endpoint.toSockaddr(storage);
            REQUIRE_EQ(length, endpoint.address.isV4() ? sizeof(sockaddr_in) : sizeof(sockaddr_in6));
            REQUIRE_EQ(storage.ss_family, endpoint.address.family());
            auto const back = Endpoint::fromSockaddr(reinterpret_cast<sockaddr const*>(&storage), length);
            REQUIRE(back);
            REQUIRE_EQ(*back, endpoint);
            REQUIRE_FALSE(Endpoint::fromSockaddr(reinterpret_cast<sockaddr const*>(&storage), length - 1));
        }
        sockaddr local{};
        local.sa_family = AF_UNIX;
        REQUIRE_FALSE(Endpoint::fromSockaddr(&local, sizeof(local)));
        REQUIRE_FALSE(Endpoint::fromSockaddr(nullptr, 0));
    }

    TEST_CASE("std::format and hashing") {
        auto const endpoint = *Endpoint::parse("[::1]:80");
        REQUIRE_EQ(std::format("{}", endpoint), "[::1]:80");
        REQUIRE_EQ(std::format("<{:>12}>", IPv4Address::loopback()), "<   127.0.0.1>");
        REQUIRE_EQ(std::format("{}", IPAddress{IPv4Address{1, 2, 3, 4}}), "1.2.3.4");

        std::unordered_set<Endpoint> endpoints{endpoint, *Endpoint::parse("1.2.3.4:80"), endpoint};
        REQUIRE_EQ(endpoints.size(), 2);
        std::unordered_set<IPAddress> addresses{IPv4Address{1, 2, 3, 4}, IPv6Address::loopback()};
        REQUIRE(addresses.contains(IPv6Address::loopback()));
    }

    TEST_CASE("resolve") {
        auto const numeric = resolve("127.0.0.1", 9000);
        REQUIRE(numeric);
        REQUIRE_EQ(numeric->size(), 1);
        REQUIRE_EQ(numeric->front(), Endpoint{IPv4Address::loopback(), 9000});

        auto const v4only = resolve("localhost", 80, AF_INET);
        REQUIRE(v4only);
        for (auto const& endpoint : *v4only) {
            REQUIRE(endpoint.address.isV4());
            REQUIRE(endpoint.address.isLoopback());
            REQUIRE_EQ(endpoint.port, 80);
        }

        auto const missing = resolve("no-such-host.invalid", 1);
        REQUIRE_FALSE(missing);
        REQUIRE_EQ(missing.error(), makeErrorCode(Error::AddressResolutionFailed));
    }
}

} // namespace turboq::reactor::testing
