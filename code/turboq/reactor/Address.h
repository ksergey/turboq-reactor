// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <sys/socket.h>

#include <array>
#include <cassert>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <variant>
#include <vector>

namespace turboq::reactor {

/// IPv4 address, stored in network byte order.
class IPv4Address {
public:
    using Bytes = std::array<std::uint8_t, 4>;

private:
    Bytes bytes_{};

public:
    /// 0.0.0.0
    constexpr IPv4Address() noexcept = default;

    constexpr explicit IPv4Address(Bytes const& bytes) noexcept : bytes_{bytes} {}

    constexpr IPv4Address(std::uint8_t a, std::uint8_t b, std::uint8_t c, std::uint8_t d) noexcept
        : bytes_{a, b, c, d} {}

    /// From a host byte order integer: IPv4Address{0x7F000001} is 127.0.0.1.
    constexpr explicit IPv4Address(std::uint32_t value) noexcept
        : bytes_{static_cast<std::uint8_t>(value >> 24), static_cast<std::uint8_t>(value >> 16),
              static_cast<std::uint8_t>(value >> 8), static_cast<std::uint8_t>(value)} {}

    [[nodiscard]] static constexpr auto any() noexcept -> IPv4Address {
        return {};
    }

    [[nodiscard]] static constexpr auto loopback() noexcept -> IPv4Address {
        return {127, 0, 0, 1};
    }

    [[nodiscard]] static constexpr auto broadcast() noexcept -> IPv4Address {
        return {255, 255, 255, 255};
    }

    /// Dotted decimal ("192.168.1.1"). Error::InvalidAddress otherwise.
    [[nodiscard]] static auto parse(std::string_view text) noexcept -> std::expected<IPv4Address, std::error_code>;

    /// Network byte order.
    [[nodiscard]] constexpr auto bytes() const noexcept -> Bytes const& {
        return bytes_;
    }

    /// Host byte order.
    [[nodiscard]] constexpr auto toUint() const noexcept -> std::uint32_t {
        return std::uint32_t{bytes_[0]} << 24 | std::uint32_t{bytes_[1]} << 16 | std::uint32_t{bytes_[2]} << 8 |
               bytes_[3];
    }

    [[nodiscard]] constexpr auto isUnspecified() const noexcept -> bool {
        return toUint() == 0;
    }

    /// 127.0.0.0/8
    [[nodiscard]] constexpr auto isLoopback() const noexcept -> bool {
        return bytes_[0] == 127;
    }

    /// 224.0.0.0/4
    [[nodiscard]] constexpr auto isMulticast() const noexcept -> bool {
        return (bytes_[0] & 0xF0) == 0xE0;
    }

    constexpr auto operator<=>(IPv4Address const&) const noexcept = default;
};

/// IPv6 address (network byte order) with an optional scope id (interface index of a link-local
/// address; written as "fe80::1%eth0" or "fe80::1%2").
class IPv6Address {
public:
    using Bytes = std::array<std::uint8_t, 16>;

private:
    Bytes bytes_{};
    std::uint32_t scopeId_{0};

public:
    /// ::
    constexpr IPv6Address() noexcept = default;

    constexpr explicit IPv6Address(Bytes const& bytes, std::uint32_t scopeId = 0) noexcept
        : bytes_{bytes}, scopeId_{scopeId} {}

    [[nodiscard]] static constexpr auto any() noexcept -> IPv6Address {
        return {};
    }

    [[nodiscard]] static constexpr auto loopback() noexcept -> IPv6Address {
        return IPv6Address{Bytes{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}};
    }

    /// ::ffff:a.b.c.d
    [[nodiscard]] static constexpr auto v4Mapped(IPv4Address const& address) noexcept -> IPv6Address {
        auto const& v4 = address.bytes();
        return IPv6Address{Bytes{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, v4[0], v4[1], v4[2], v4[3]}};
    }

    /// RFC 4291 text ("2001:db8::1", "::ffff:10.0.0.1"), optionally with "%<interface name or
    /// index>". Error::InvalidAddress otherwise.
    [[nodiscard]] static auto parse(std::string_view text) noexcept -> std::expected<IPv6Address, std::error_code>;

    /// Network byte order.
    [[nodiscard]] constexpr auto bytes() const noexcept -> Bytes const& {
        return bytes_;
    }

    [[nodiscard]] constexpr auto scopeId() const noexcept -> std::uint32_t {
        return scopeId_;
    }

    [[nodiscard]] constexpr auto isUnspecified() const noexcept -> bool {
        return *this == IPv6Address{Bytes{}, scopeId_};
    }

    /// ::1
    [[nodiscard]] constexpr auto isLoopback() const noexcept -> bool {
        return bytes_ == loopback().bytes_;
    }

    /// ff00::/8
    [[nodiscard]] constexpr auto isMulticast() const noexcept -> bool {
        return bytes_[0] == 0xFF;
    }

    /// fe80::/10
    [[nodiscard]] constexpr auto isLinkLocal() const noexcept -> bool {
        return bytes_[0] == 0xFE && (bytes_[1] & 0xC0) == 0x80;
    }

    [[nodiscard]] constexpr auto isV4Mapped() const noexcept -> bool {
        for (std::size_t i = 0; i < 10; ++i) {
            if (bytes_[i] != 0) {
                return false;
            }
        }
        return bytes_[10] == 0xFF && bytes_[11] == 0xFF;
    }

    /// The IPv4 address of a v4-mapped address.
    [[nodiscard]] constexpr auto toV4() const noexcept -> std::optional<IPv4Address> {
        if (!isV4Mapped()) {
            return std::nullopt;
        }
        return IPv4Address{bytes_[12], bytes_[13], bytes_[14], bytes_[15]};
    }

    constexpr auto operator<=>(IPv6Address const&) const noexcept = default;
};

/// IPv4 or IPv6 address. Default: 0.0.0.0.
class IPAddress {
private:
    std::variant<IPv4Address, IPv6Address> value_{};

public:
    constexpr IPAddress() noexcept = default;

    constexpr IPAddress(IPv4Address const& address) noexcept : value_{address} {}

    constexpr IPAddress(IPv6Address const& address) noexcept : value_{address} {}

    /// IPv4 or IPv6 text (see IPv4Address::parse(), IPv6Address::parse()).
    [[nodiscard]] static auto parse(std::string_view text) noexcept -> std::expected<IPAddress, std::error_code>;

    [[nodiscard]] constexpr auto isV4() const noexcept -> bool {
        return value_.index() == 0;
    }

    [[nodiscard]] constexpr auto isV6() const noexcept -> bool {
        return value_.index() == 1;
    }

    /// The IPv4 address. Requires isV4().
    [[nodiscard]] constexpr auto v4() const noexcept -> IPv4Address const& {
        assert(isV4());
        return *std::get_if<IPv4Address>(&value_);
    }

    /// The IPv6 address. Requires isV6().
    [[nodiscard]] constexpr auto v6() const noexcept -> IPv6Address const& {
        assert(isV6());
        return *std::get_if<IPv6Address>(&value_);
    }

    /// AF_INET or AF_INET6.
    [[nodiscard]] constexpr auto family() const noexcept -> int {
        return isV4() ? AF_INET : AF_INET6;
    }

    [[nodiscard]] constexpr auto isUnspecified() const noexcept -> bool {
        return isV4() ? v4().isUnspecified() : v6().isUnspecified();
    }

    [[nodiscard]] constexpr auto isLoopback() const noexcept -> bool {
        return isV4() ? v4().isLoopback() : v6().isLoopback();
    }

    [[nodiscard]] constexpr auto isMulticast() const noexcept -> bool {
        return isV4() ? v4().isMulticast() : v6().isMulticast();
    }

    /// Visit the underlying IPv4Address / IPv6Address.
    template <typename Visitor>
    constexpr decltype(auto) visit(Visitor&& visitor) const {
        return std::visit(std::forward<Visitor>(visitor), value_);
    }

    constexpr auto operator<=>(IPAddress const&) const noexcept = default;
};

/// IP address and port: "1.2.3.4:80", "[2001:db8::1]:443", "[fe80::1%eth0]:5001".
struct Endpoint {
    IPAddress address{};
    std::uint16_t port{0};

    /// Error::InvalidAddress unless `text` is "<IPv4>:<port>" or "[<IPv6>]:<port>".
    [[nodiscard]] static auto parse(std::string_view text) noexcept -> std::expected<Endpoint, std::error_code>;

    /// From a sockaddr_in / sockaddr_in6. std::nullopt for other families or a short length.
    [[nodiscard]] static auto fromSockaddr(sockaddr const* address, socklen_t length) noexcept
        -> std::optional<Endpoint>;

    /// Fill a sockaddr_in / sockaddr_in6, return its length.
    auto toSockaddr(sockaddr_storage& storage) const noexcept -> socklen_t;

    constexpr auto operator<=>(Endpoint const&) const noexcept = default;
};

// Text conversion.

[[nodiscard]] auto toString(IPv4Address const& address) -> std::string;
/// Scope written as the interface name when it exists, the numeric index otherwise.
[[nodiscard]] auto toString(IPv6Address const& address) -> std::string;
[[nodiscard]] auto toString(IPAddress const& address) -> std::string;
[[nodiscard]] auto toString(Endpoint const& endpoint) -> std::string;

/// Resolve a host name or numeric address with getaddrinfo() (blocking: keep it off latency
/// critical threads). Unique endpoints in resolver order. Error::AddressResolutionFailed if there
/// is none.
[[nodiscard]] auto resolve(std::string const& host, std::uint16_t port, int family = AF_UNSPEC)
    -> std::expected<std::vector<Endpoint>, std::error_code>;

} // namespace turboq::reactor

// std::format / std::print support.

template <typename T>
    requires std::same_as<T, turboq::reactor::IPv4Address> || std::same_as<T, turboq::reactor::IPv6Address> ||
             std::same_as<T, turboq::reactor::IPAddress> || std::same_as<T, turboq::reactor::Endpoint>
struct std::formatter<T> : std::formatter<std::string_view> {
    auto format(T const& value, std::format_context& context) const {
        auto const text = turboq::reactor::toString(value);
        return std::formatter<std::string_view>::format(text, context);
    }
};

// Hashing (unordered containers).

template <>
struct std::hash<turboq::reactor::IPv4Address> {
    auto operator()(turboq::reactor::IPv4Address const& address) const noexcept -> std::size_t {
        return std::hash<std::uint32_t>{}(address.toUint());
    }
};

template <>
struct std::hash<turboq::reactor::IPv6Address> {
    auto operator()(turboq::reactor::IPv6Address const& address) const noexcept -> std::size_t {
        std::size_t hash = address.scopeId();
        for (auto const byte : address.bytes()) {
            hash = hash * 131 + byte;
        }
        return hash;
    }
};

template <>
struct std::hash<turboq::reactor::IPAddress> {
    auto operator()(turboq::reactor::IPAddress const& address) const noexcept -> std::size_t {
        return address.visit([](auto const& value) {
            return std::hash<std::remove_cvref_t<decltype(value)>>{}(value);
        });
    }
};

template <>
struct std::hash<turboq::reactor::Endpoint> {
    auto operator()(turboq::reactor::Endpoint const& endpoint) const noexcept -> std::size_t {
        return std::hash<turboq::reactor::IPAddress>{}(endpoint.address) * 65537 + endpoint.port;
    }
};
