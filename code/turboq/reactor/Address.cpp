// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "Address.h"

#include <arpa/inet.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>

#include <algorithm>
#include <charconv>
#include <cstring>

#include "Error.h"

namespace turboq::reactor {
namespace {

[[nodiscard]] auto invalid() noexcept -> std::unexpected<std::error_code> {
    return std::unexpected(makeErrorCode(Error::InvalidAddress));
}

/// inet_pton() needs a NUL-terminated string.
template <std::size_t N>
[[nodiscard]] auto copyTerminated(std::string_view text, char (&buffer)[N]) noexcept -> bool {
    if (text.empty() || text.size() >= N || text.find('\0') != std::string_view::npos) {
        return false;
    }
    std::memcpy(buffer, text.data(), text.size());
    buffer[text.size()] = '\0';
    return true;
}

[[nodiscard]] auto parsePort(std::string_view text) noexcept -> std::optional<std::uint16_t> {
    std::uint16_t port = 0;
    auto const [end, ec] = std::from_chars(text.data(), text.data() + text.size(), port);
    if (text.empty() || ec != std::errc{} || end != text.data() + text.size()) {
        return std::nullopt;
    }
    return port;
}

} // namespace

auto IPv4Address::parse(std::string_view text) noexcept -> std::expected<IPv4Address, std::error_code> {
    char buffer[INET_ADDRSTRLEN];
    Bytes bytes;
    if (!copyTerminated(text, buffer) || ::inet_pton(AF_INET, buffer, bytes.data()) != 1) {
        return invalid();
    }
    return IPv4Address{bytes};
}

auto IPv6Address::parse(std::string_view text) noexcept -> std::expected<IPv6Address, std::error_code> {
    std::uint32_t scopeId = 0;
    if (auto const percent = text.find('%'); percent != std::string_view::npos) {
        auto const scope = text.substr(percent + 1);
        text = text.substr(0, percent);
        if (scope.empty()) {
            return invalid();
        }
        auto const [end, ec] = std::from_chars(scope.data(), scope.data() + scope.size(), scopeId);
        if (ec != std::errc{} || end != scope.data() + scope.size()) {
            char name[IF_NAMESIZE];
            if (!copyTerminated(scope, name) || (scopeId = ::if_nametoindex(name)) == 0) {
                return invalid();
            }
        }
    }
    char buffer[INET6_ADDRSTRLEN];
    Bytes bytes;
    if (!copyTerminated(text, buffer) || ::inet_pton(AF_INET6, buffer, bytes.data()) != 1) {
        return invalid();
    }
    return IPv6Address{bytes, scopeId};
}

auto IPAddress::parse(std::string_view text) noexcept -> std::expected<IPAddress, std::error_code> {
    if (text.find(':') == std::string_view::npos) {
        return IPv4Address::parse(text).transform([](IPv4Address const& v4) {
            return IPAddress{v4};
        });
    }
    return IPv6Address::parse(text).transform([](IPv6Address const& v6) {
        return IPAddress{v6};
    });
}

auto Endpoint::parse(std::string_view text) noexcept -> std::expected<Endpoint, std::error_code> {
    std::string_view host;
    std::string_view port;
    if (text.starts_with('[')) {
        auto const close = text.find("]:");
        if (close == std::string_view::npos) {
            return invalid();
        }
        host = text.substr(1, close - 1);
        port = text.substr(close + 2);
    } else {
        auto const colon = text.rfind(':');
        if (colon == std::string_view::npos) {
            return invalid();
        }
        host = text.substr(0, colon);
        port = text.substr(colon + 1);
        if (host.find(':') != std::string_view::npos) {
            return invalid(); // IPv6 needs brackets
        }
    }
    auto const portValue = parsePort(port);
    if (!portValue) {
        return invalid();
    }
    bool const bracketed = text.starts_with('[');
    if (bracketed) {
        auto address = IPv6Address::parse(host);
        if (!address) {
            return std::unexpected(address.error());
        }
        return Endpoint{*address, *portValue};
    }
    auto address = IPv4Address::parse(host);
    if (!address) {
        return std::unexpected(address.error());
    }
    return Endpoint{*address, *portValue};
}

auto Endpoint::fromSockaddr(sockaddr const* address, socklen_t length) noexcept -> std::optional<Endpoint> {
    if (address == nullptr) {
        return std::nullopt;
    }
    if (address->sa_family == AF_INET && length >= static_cast<socklen_t>(sizeof(sockaddr_in))) {
        sockaddr_in in;
        std::memcpy(&in, address, sizeof(in));
        IPv4Address::Bytes bytes;
        std::memcpy(bytes.data(), &in.sin_addr, bytes.size());
        return Endpoint{IPv4Address{bytes}, ntohs(in.sin_port)};
    }
    if (address->sa_family == AF_INET6 && length >= static_cast<socklen_t>(sizeof(sockaddr_in6))) {
        sockaddr_in6 in6;
        std::memcpy(&in6, address, sizeof(in6));
        IPv6Address::Bytes bytes;
        std::memcpy(bytes.data(), &in6.sin6_addr, bytes.size());
        return Endpoint{IPv6Address{bytes, in6.sin6_scope_id}, ntohs(in6.sin6_port)};
    }
    return std::nullopt;
}

auto Endpoint::toSockaddr(sockaddr_storage& storage) const noexcept -> socklen_t {
    std::memset(&storage, 0, sizeof(storage));
    if (address.isV4()) {
        sockaddr_in in{};
        in.sin_family = AF_INET;
        in.sin_port = htons(port);
        std::memcpy(&in.sin_addr, address.v4().bytes().data(), 4);
        std::memcpy(&storage, &in, sizeof(in));
        return sizeof(in);
    }
    sockaddr_in6 in6{};
    in6.sin6_family = AF_INET6;
    in6.sin6_port = htons(port);
    std::memcpy(&in6.sin6_addr, address.v6().bytes().data(), 16);
    in6.sin6_scope_id = address.v6().scopeId();
    std::memcpy(&storage, &in6, sizeof(in6));
    return sizeof(in6);
}

auto toString(IPv4Address const& address) -> std::string {
    char buffer[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, address.bytes().data(), buffer, sizeof(buffer));
    return buffer;
}

auto toString(IPv6Address const& address) -> std::string {
    char buffer[INET6_ADDRSTRLEN];
    ::inet_ntop(AF_INET6, address.bytes().data(), buffer, sizeof(buffer));
    std::string result = buffer;
    if (address.scopeId() != 0) {
        result += '%';
        char name[IF_NAMESIZE];
        if (::if_indextoname(address.scopeId(), name) != nullptr) {
            result += name;
        } else {
            result += std::to_string(address.scopeId());
        }
    }
    return result;
}

auto toString(IPAddress const& address) -> std::string {
    return address.visit([](auto const& value) {
        return toString(value);
    });
}

auto toString(Endpoint const& endpoint) -> std::string {
    if (endpoint.address.isV4()) {
        return toString(endpoint.address.v4()) + ':' + std::to_string(endpoint.port);
    }
    return '[' + toString(endpoint.address.v6()) + "]:" + std::to_string(endpoint.port);
}

auto resolve(
    std::string const& host, std::uint16_t port, int family) -> std::expected<std::vector<Endpoint>, std::error_code> {
    addrinfo hints{};
    hints.ai_family = family;
    hints.ai_socktype = SOCK_STREAM; // one entry per address instead of one per socket type
    hints.ai_flags = AI_NUMERICSERV;

    auto const service = std::to_string(port);
    addrinfo* result = nullptr;
    if (::getaddrinfo(host.c_str(), service.c_str(), &hints, &result) != 0 || result == nullptr) {
        return std::unexpected(makeErrorCode(Error::AddressResolutionFailed));
    }
    std::vector<Endpoint> endpoints;
    for (auto const* info = result; info != nullptr; info = info->ai_next) {
        if (auto endpoint = Endpoint::fromSockaddr(info->ai_addr, info->ai_addrlen);
            endpoint && std::ranges::find(endpoints, *endpoint) == endpoints.end()) {
            endpoints.push_back(*endpoint);
        }
    }
    ::freeaddrinfo(result);
    if (endpoints.empty()) {
        return std::unexpected(makeErrorCode(Error::AddressResolutionFailed));
    }
    return endpoints;
}

} // namespace turboq::reactor
